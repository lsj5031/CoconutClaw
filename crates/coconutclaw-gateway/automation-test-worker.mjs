import worker, { ControlLedger as BaseLedger, ControlOperator } from "./control-worker.mjs";

export { worker as default, ControlOperator };

export class ControlLedger extends BaseLedger {
    constructor(ctx, env) {
        super(ctx, env);
        const advance = this.automation.advance.bind(this.automation);
        this.automation.advance = (...args) => {
            advance(...args);
            this.interrupt("admission");
        };
        const finish = this.automation.finish.bind(this.automation);
        this.automation.finish = (run, outcome, value) => {
            finish(run, outcome, value);
            this.interrupt(outcome);
        };
    }

    interrupt(point) {
        if (this.fault === point) {
            this.fault = null;
            throw new Error(`fixture interruption at ${point}`);
        }
    }

    automationNow() {
        return this.clock ?? super.automationNow();
    }

    stageAutomationAlarm(at) {
        const delay = this.alarmDelay ?? 0;
        if (this.clock !== null && this.clock !== undefined && at !== null) {
            at = Date.now() + 60 * 60_000 + Math.max(0, at - this.clock);
        }
        const alarm = super.stageAutomationAlarm(at === null ? null : at + delay);
        this.interrupt("alarm");
        return alarm;
    }

    async fixtureCommand({ action, value }) {
        if (action === "clock") this.clock = value;
        if (action === "delay-alarm") this.alarmDelay = value;
        if (action === "fault") this.fault = value;
        if (action === "wake") await this.alarm();
        if (action === "pending-alarm") return this.ctx.storage.getAlarm();
    }
}
