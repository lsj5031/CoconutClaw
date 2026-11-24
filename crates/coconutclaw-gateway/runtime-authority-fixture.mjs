import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { isDeepStrictEqual } from "node:util";

export const origin = "https://control.invalid";
export const hostname = "bigfur.invalid";
export const bearer = "a".repeat(64);

export function authorityFixture(path) {
    const db = new DatabaseSync(path);
    db.exec(`
        PRAGMA synchronous=FULL;
        CREATE TABLE IF NOT EXISTS authority (
            generation INTEGER PRIMARY KEY, record TEXT NOT NULL, closed INTEGER NOT NULL
        ) STRICT;
        CREATE TABLE IF NOT EXISTS sends (
            generation INTEGER NOT NULL, task TEXT NOT NULL, number INTEGER NOT NULL,
            PRIMARY KEY(generation,task,number)
        ) STRICT;
    `);
    const checks = [];
    const sends = [];
    const fixture = {
        checks,
        sends,
        beforeReply: undefined,
        beforeSend: undefined,
        close: () => db.close(),
        get current() {
            return db.prepare("SELECT * FROM authority ORDER BY generation DESC LIMIT 1").get();
        },
        publish(target, objectIncarnation, runnerIncarnation) {
            db.exec("BEGIN IMMEDIATE");
            try {
                const previous = fixture.current;
                assert.equal(target.pin.generation, (previous?.generation ?? 0) + 1);
                assert.ok(!previous || previous.closed);
                db.prepare("INSERT INTO authority VALUES (?, ?, 0)").run(
                    target.pin.generation, JSON.stringify({ target, objectIncarnation, runnerIncarnation }),
                );
                db.exec("COMMIT");
            } catch (error) {
                db.exec("ROLLBACK");
                throw error;
            }
        },
        retire() {
            db.prepare("UPDATE authority SET closed=1 WHERE generation=?").run(fixture.current.generation);
        },
        async fetch(request) {
            const url = new URL(request.url);
            assert.equal(url.origin, origin);
            assert.equal(request.method, "POST");
            assert.equal(request.headers.get("authorization"), `Bearer ${bearer}`);
            const { nonce, target, objectIncarnation, runnerIncarnation, delivery } = await request.json();
            const authorized = () => {
                const current = fixture.current;
                if (!current || current.closed) return false;
                const active = JSON.parse(current.record);
                return isDeepStrictEqual(target, active.target) && objectIncarnation === active.objectIncarnation &&
                    (runnerIncarnation === null || runnerIncarnation === active.runnerIncarnation);
            };
            if (url.pathname === "/v1/check") {
                assert.equal(delivery, null);
                const allowed = authorized();
                checks.push({ target, objectIncarnation, runnerIncarnation, allowed });
                if (!allowed) return new Response(null, { status: 503 });
                const reply = {
                    nonce, target, highWater: fixture.current.generation,
                    objectIncarnation, runnerIncarnation, validUntil: Date.now() + 900,
                };
                const inject = await fixture.beforeReply?.(reply);
                return inject ?? Response.json(reply);
            }
            assert.equal(url.pathname, "/v1/send");
            assert.equal(runnerIncarnation, null);
            await fixture.beforeSend?.();
            if (!authorized()) return new Response(null, { status: 503 });
            const inserted = db.prepare("INSERT OR IGNORE INTO sends VALUES (?, ?, ?)").run(
                target.pin.generation, delivery.task, delivery.sendNumber,
            );
            if (!inserted.changes) return new Response(null, { status: 409 });
            sends.push(delivery);
            return Response.json({ nonce, messageId: sends.length });
        },
    };
    return fixture;
}
