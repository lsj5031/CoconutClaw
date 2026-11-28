import { Denied } from "./authority-core.mjs";
import { fields } from "./authority-preflight.mjs";

export const day = 86_400_000;
export const catchUpWindow = 2 * day;
const minute = 60_000;
const repositoryName = value => typeof value === "string" &&
    /^[a-z0-9][a-z0-9-]{0,38}\/[a-z0-9][a-z0-9._-]{0,99}$/.test(value);

function formatter(zone) {
    return new Intl.DateTimeFormat("en-GB", {
        timeZone: zone, calendar: "iso8601", numberingSystem: "latn", hourCycle: "h23",
        year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit",
        second: "2-digit",
    });
}

export function parseSchedule(value, repository) {
    if (!fields(value, ["repository", "zone", "time", "gap", "fold", "chatId", "messageThreadId"]) ||
        !repositoryName(value.repository) || value.repository !== repository ||
        typeof value.zone !== "string" || value.zone.length > 80 ||
        !/^(?:UTC|[A-Za-z_+-]+(?:\/[A-Za-z0-9_+-]+)+)$/.test(value.zone) ||
        typeof value.time !== "string" || !/^(?:[01]\d|2[0-3]):[0-5]\d$/.test(value.time) ||
        value.gap !== "skip" || !["earlier", "later"].includes(value.fold) ||
        !Number.isSafeInteger(value.chatId) || value.chatId === 0 ||
        !Number.isInteger(value.messageThreadId) || value.messageThreadId < 1 ||
        value.messageThreadId > 0x7fff_ffff) throw new Denied(400);
    try {
        formatter(value.zone);
    } catch {
        throw new Denied(400);
    }
    const { zone, time, gap, fold, chatId, messageThreadId } = value;
    return { repository, zone, time, gap, fold, chatId, messageThreadId };
}

function localTime(format, instant) {
    const parts = Object.fromEntries(format.formatToParts(instant).map(({ type, value }) => [type, value]));
    return Date.UTC(parts.year, Number(parts.month) - 1, parts.day, parts.hour, parts.minute, parts.second);
}

function resolveLocal(format, wallTime, fold) {
    const offsets = new Set();
    for (let delta = -36; delta <= 36; delta += 12) {
        const sample = wallTime + delta * 60 * minute;
        offsets.add(localTime(format, sample) - sample);
    }
    const matching = [...offsets].map(offset => wallTime - offset)
        .filter(instant => localTime(format, instant) === wallTime);
    if (!matching.length) return null;
    return fold === "earlier" ? Math.min(...matching) : Math.max(...matching);
}

export function nextTimes(config, after, count = 1, onGap = () => {}) {
    const format = formatter(config.zone);
    const [hours, minutes] = config.time.split(":").map(Number);
    const wallNow = localTime(format, after);
    const firstDate = Math.floor(wallNow / day) * day;
    const times = [];
    for (let index = 0; times.length < count && index < count + 7; index++) {
        const wallTime = firstDate + index * day + (hours * 60 + minutes) * minute;
        const instant = resolveLocal(format, wallTime, config.fold);
        const local = new Date(wallTime).toISOString().slice(0, 16);
        if (instant !== null && instant > after) times.push({ instant, local });
        if (instant === null && wallTime > wallNow) onGap(local);
    }
    if (times.length !== count) throw new Error("daily schedule could not be resolved");
    return times;
}

const conclusions = ["queued", "running", "success", "failure", "cancelled", "neutral", "skipped", "timed_out"];

export function renderDigest(repository, metadata) {
    if (!fields(metadata, ["runs"]) || !Array.isArray(metadata.runs) || metadata.runs.length > 10 ||
        !metadata.runs.every(run => fields(run, ["name", "status", "sha"]) &&
            typeof run.name === "string" && /^[\p{L}\p{N} ._()/:-]{1,80}$/u.test(run.name) &&
            conclusions.includes(run.status) && typeof run.sha === "string" && /^[a-f0-9]{40}$/.test(run.sha))) {
        throw new Denied(400);
    }
    const lines = metadata.runs.map(({ name, status, sha }) => `${name} · ${status} · ${sha.slice(0, 7)}`);
    return [`CI digest · ${repository}`, ...lines.length ? lines : ["No recent runs."]].join("\n");
}
