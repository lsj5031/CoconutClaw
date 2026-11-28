import { Denied, positiveInteger } from "./authority-core.mjs";
import { fields, productionPairing, targetFields } from "./authority-preflight.mjs";
import { catchUpWindow, day, nextTimes, parseSchedule, renderDigest } from "./automation-digest.mjs";

const quota = 8;
const scheduleLimit = 16;
const wakeLimit = 4;
const runLifetime = 10 * 60_000;
const scheduleId = value => typeof value === "string" && /^[a-z][a-z0-9-]{0,39}$/.test(value);
const manualKey = value => typeof value === "string" && /^[a-zA-Z0-9_-]{1,80}$/.test(value);
const nextAt = (config, after) => nextTimes(config, after)[0].instant;

export const automationSchema = `
    CREATE TABLE IF NOT EXISTS automation_gate (
        generation INTEGER PRIMARY KEY REFERENCES publications,
        frozen INTEGER NOT NULL CHECK (frozen IN (0, 1))
    ) STRICT;
    CREATE TABLE IF NOT EXISTS automation_schedules (
        id TEXT PRIMARY KEY,
        generation INTEGER NOT NULL REFERENCES publications,
        revision INTEGER NOT NULL,
        version INTEGER NOT NULL,
        config TEXT NOT NULL,
        paused INTEGER NOT NULL CHECK (paused IN (0, 1)),
        next_at INTEGER NOT NULL
    ) STRICT;
    CREATE TABLE IF NOT EXISTS automation_runs (
        schedule_id TEXT NOT NULL REFERENCES automation_schedules,
        run_key TEXT NOT NULL,
        revision INTEGER NOT NULL,
        generation INTEGER NOT NULL REFERENCES publications,
        config TEXT NOT NULL,
        due_at INTEGER,
        recorded_at INTEGER NOT NULL,
        quota_day INTEGER,
        deadline INTEGER,
        execution TEXT NOT NULL,
        delivery TEXT NOT NULL,
        reason TEXT,
        digest TEXT,
        message_id INTEGER,
        PRIMARY KEY (schedule_id, run_key),
        CHECK ((execution = 'skipped' AND delivery = 'none' AND quota_day IS NULL AND deadline IS NULL)
            OR (execution = 'failed' AND delivery = 'none' AND quota_day IS NOT NULL AND deadline IS NOT NULL)
            OR (execution = 'pending' AND delivery = 'waiting' AND quota_day IS NOT NULL AND deadline IS NOT NULL)
            OR (execution = 'ready' AND delivery IN ('waiting', 'unknown', 'sent', 'blocked')
                AND quota_day IS NOT NULL AND deadline IS NOT NULL)),
        CHECK ((execution = 'ready') = (digest IS NOT NULL)),
        CHECK ((delivery = 'sent') = (message_id IS NOT NULL))
    ) STRICT;
    CREATE UNIQUE INDEX IF NOT EXISTS automation_one_open ON automation_runs(schedule_id)
        WHERE delivery IN ('waiting', 'unknown');
    CREATE INDEX IF NOT EXISTS automation_quota ON automation_runs(generation, quota_day);
    CREATE INDEX IF NOT EXISTS automation_waiting ON automation_runs(recorded_at, schedule_id, run_key)
        WHERE delivery = 'waiting';
    CREATE INDEX IF NOT EXISTS automation_history ON automation_runs(schedule_id, recorded_at DESC, run_key);
`;

const withConfig = row => ({ ...row, config: JSON.parse(row.config) });
const publicRun = ({ digest, ...row }) => withConfig(row);

export class AutomationLedger {
    constructor(store) {
        this.store = store;
        this.db = store.db;
    }

    schedule(id) {
        const row = this.db.prepare("SELECT * FROM automation_schedules WHERE id=?").get(id);
        if (!row) throw new Denied(404);
        return row;
    }

    generation(credentialVersion) {
        const { generation, publication } = this.store.active();
        if (publication.target.pairing !== productionPairing ||
            publication.target.telegramCredentialVersion !== credentialVersion) throw new Denied(503);
        return generation;
    }

    enforceGate(generation, context) {
        if (generation !== this.generation(context.credentialVersion) || !context.sender ||
            this.db.prepare("SELECT frozen FROM automation_gate WHERE generation=?").get(generation)?.frozen !== 0) {
            throw new Denied(503);
        }
    }

    operate(action, body, context, now) {
        if (action === "freeze") {
            if (!fields(body, ["generation", "frozen"]) || !targetFields.generation(body.generation) ||
                typeof body.frozen !== "boolean") throw new Denied(400);
            if (body.generation !== this.generation(context.credentialVersion)) throw new Denied(409);
            this.db.prepare(`INSERT INTO automation_gate VALUES (?,?)
                ON CONFLICT(generation) DO UPDATE SET frozen=excluded.frozen`).run(body.generation, Number(body.frozen));
            return body;
        }
        if (action === "dry-run") {
            if (!fields(body, ["config", "metadata"])) throw new Denied(400);
            const config = parseSchedule(body.config, context.repository);
            return { config, digest: renderDigest(config.repository, body.metadata) };
        }
        if (!scheduleId(body?.id)) throw new Denied(400);
        if (action === "save") return this.save(body, context, now);
        const schedule = this.schedule(body.id);
        const config = JSON.parse(schedule.config);
        if (action === "preview") {
            if (!fields(body, ["id"])) throw new Denied(400);
            const gaps = [];
            const times = nextTimes(config, now, 5, local => gaps.push(local));
            return { schedule: withConfig(schedule), times, gaps };
        }
        if (action === "history") {
            if (!fields(body, ["id"])) throw new Denied(400);
            return { runs: this.db.prepare(`SELECT * FROM automation_runs WHERE schedule_id=?
                ORDER BY recorded_at DESC, run_key LIMIT 50`).all(body.id).map(publicRun) };
        }
        if (action === "run-once") return this.runOnce(body, schedule, context, now);
        const resume = action === "resume";
        if (!fields(body, resume ? ["id", "version", "backfill"] : ["id", "version"]) ||
            !positiveInteger(body.version) || (resume && typeof body.backfill !== "boolean")) throw new Denied(400);
        if (body.version !== schedule.version) throw new Denied(409);
        if (resume) this.enforceGate(schedule.generation, context);
        if (schedule.paused === Number(!resume)) return withConfig(schedule);
        const cursor = resume && !body.backfill ? Math.max(schedule.next_at, nextAt(config, now)) : schedule.next_at;
        if (cursor > schedule.next_at && schedule.next_at <= now) {
            this.skip(schedule, schedule.next_at, now, "resume_next_future");
        }
        this.db.prepare("UPDATE automation_schedules SET version=version+1, paused=?, next_at=? WHERE id=?")
            .run(Number(!resume), cursor, body.id);
        return withConfig(this.schedule(body.id));
    }

    save(body, context, now) {
        if (!fields(body, ["id", "version", "config"]) || (body.version !== 0 && !positiveInteger(body.version))) {
            throw new Denied(400);
        }
        const config = parseSchedule(body.config, context.repository);
        const generation = this.generation(context.credentialVersion);
        const old = this.db.prepare("SELECT * FROM automation_schedules WHERE id=?").get(body.id);
        if ((old?.version ?? 0) !== body.version) throw new Denied(409);
        if (!old && this.db.prepare("SELECT COUNT(*) AS total FROM automation_schedules").get().total >= scheduleLimit) {
            throw new Denied(429);
        }
        if (old && old.generation === generation && old.config === JSON.stringify(config)) return withConfig(old);
        if (old && old.next_at <= now) this.skip(old, old.next_at, now, "revision_replaced");
        this.db.prepare(`INSERT INTO automation_schedules (id,generation,revision,version,config,paused,next_at)
            VALUES (?,?,1,1,?,1,?) ON CONFLICT(id) DO UPDATE SET generation=excluded.generation,
            revision=revision+1, version=version+1, config=excluded.config, paused=1, next_at=excluded.next_at`)
            .run(body.id, generation, JSON.stringify(config), nextAt(config, now));
        return withConfig(this.schedule(body.id));
    }

    runOnce(body, schedule, context, now) {
        if (!fields(body, ["id", "revision", "key"]) || !manualKey(body.key) || !positiveInteger(body.revision)) {
            throw new Denied(400);
        }
        const key = `manual:${body.key}`;
        const prior = this.run(schedule.id, key);
        if (prior) {
            if (prior.revision !== body.revision) throw new Denied(409);
            return publicRun(prior);
        }
        if (schedule.revision !== body.revision) throw new Denied(409);
        this.enforceGate(schedule.generation, context);
        const reason = this.capacity(schedule, now);
        if (reason) throw new Denied(reason === "quota" ? 429 : 409);
        this.admit(schedule, key, null, now);
        return publicRun(this.run(schedule.id, key));
    }

    run(id, key) {
        return this.db.prepare("SELECT * FROM automation_runs WHERE schedule_id=? AND run_key=?").get(id, key);
    }

    capacity(schedule, now) {
        if (this.db.prepare(`SELECT 1 FROM automation_runs WHERE schedule_id=? AND delivery IN ('waiting','unknown')`)
            .get(schedule.id)) return "unresolved";
        if (this.db.prepare("SELECT COUNT(*) AS total FROM automation_runs WHERE generation=? AND quota_day=?")
            .get(schedule.generation, Math.floor(now / day)).total >= quota) return "quota";
        return null;
    }

    admit(schedule, key, due, now) {
        this.db.prepare(`INSERT INTO automation_runs
            (schedule_id,run_key,revision,generation,config,due_at,recorded_at,quota_day,deadline,execution,delivery)
            VALUES (?,?,?,?,?,?,?,?,?,'pending','waiting')`)
            .run(schedule.id, key, schedule.revision, schedule.generation, schedule.config, due, now,
                Math.floor(now / day), now + runLifetime);
    }

    skip(schedule, due, now, reason) {
        this.db.prepare(`INSERT INTO automation_runs
            (schedule_id,run_key,revision,generation,config,due_at,recorded_at,execution,delivery,reason)
            VALUES (?,?,?,?,?,?,?,'skipped','none',?)`)
            .run(schedule.id, `occurrence:${schedule.revision}:${due}`, schedule.revision,
                schedule.generation, schedule.config, due, now, reason);
    }

    advance(context, now) {
        const due = this.db.prepare(`SELECT * FROM automation_schedules WHERE paused=0 AND next_at<=?
            ORDER BY next_at,id LIMIT ?`).all(now, wakeLimit);
        for (const schedule of due) {
            const config = JSON.parse(schedule.config);
            const dueAt = schedule.next_at;
            const cutoff = now - catchUpWindow;
            if (dueAt < cutoff) {
                this.skip(schedule, dueAt, now, "catch_up_expired");
                this.move(schedule, nextAt(config, cutoff - 1));
                continue;
            }
            let reason = "control_unavailable";
            try {
                this.enforceGate(schedule.generation, context);
                reason = this.capacity(schedule, now);
            } catch (error) {
                if (!(error instanceof Denied)) throw error;
            }
            if (reason) this.skip(schedule, dueAt, now, reason);
            else this.admit(schedule, `occurrence:${schedule.revision}:${dueAt}`, dueAt, now);
            this.move(schedule, nextAt(config, dueAt));
        }
    }

    move(schedule, cursor) {
        this.db.prepare("UPDATE automation_schedules SET next_at=? WHERE id=?").run(cursor, schedule.id);
    }

    nextWake(now, dispatching) {
        const { next } = this.db.prepare("SELECT MIN(next_at) AS next FROM automation_schedules WHERE paused=0").get();
        const waiting = this.waiting();
        const deliveryAt = waiting ? (dispatching ? now + 1000 : waiting.recorded_at + 100) : Infinity;
        const earliest = Math.min(next ?? Infinity, deliveryAt);
        return earliest === Infinity ? null : Math.max(now, earliest);
    }

    waiting() {
        return this.db.prepare(`SELECT * FROM automation_runs WHERE delivery='waiting'
            ORDER BY recorded_at,schedule_id,run_key LIMIT 1`).get();
    }

    finish(run, outcome, value = null) {
        const outcomes = {
            ready: ["execution='ready', digest=?", "execution='pending' AND delivery='waiting'"],
            failed: ["execution='failed', delivery='none', reason=?", "execution='pending' AND delivery='waiting'"],
            blocked: ["delivery='blocked', reason=?", "execution='ready' AND delivery='waiting'"],
            reserve: ["delivery='unknown', reason=?", "execution='ready' AND delivery='waiting'"],
            sent: ["delivery='sent', reason=NULL, message_id=?", "delivery='unknown'"],
        };
        const [set, where] = outcomes[outcome];
        const { changes } = this.db.prepare(`UPDATE automation_runs SET ${set} WHERE schedule_id=? AND run_key=? AND ${where}`)
            .run(value, run.schedule_id, run.run_key);
        if (changes !== 1) throw new Error("invalid automation run transition");
    }
}
