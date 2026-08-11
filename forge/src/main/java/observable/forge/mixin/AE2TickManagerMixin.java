package observable.forge.mixin;

import observable.forge.compat.AE2GridProfiler;
import observable.forge.compat.CompatTiming;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Pseudo;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

/**
 * Profiles AE2 IGridTickable work which never enters Level#tickBlockEntities,
 * plus the internal TickManagerService scheduler layers used for the v10 grid
 * breakdown. All injections are optional so minor AE2 fork differences fail
 * soft instead of preventing the game from loading.
 */
@Pseudo
@Mixin(targets = "appeng.me.service.TickManagerService", remap = false)
public abstract class AE2TickManagerMixin {
    @Inject(method = "unsafeTickingRequest", at = @At("HEAD"), require = 0)
    private void observable$beginAE2Tick(CallbackInfoReturnable<Object> cir) {
        CompatTiming.beginAE2Tick(this);
    }

    @Inject(method = "unsafeTickingRequest", at = @At("RETURN"), require = 0)
    private void observable$endAE2Tick(CallbackInfoReturnable<Object> cir) {
        CompatTiming.endAE2Tick(cir.getReturnValue());
    }

    @Inject(method = "tickLevelQueue", at = @At("HEAD"), require = 0)
    private void observable$beginTickLevelQueue(CallbackInfo ci) {
        AE2GridProfiler.beginTickManagerSection(this, AE2GridProfiler.TICK_SECTION_LEVEL_QUEUE);
    }

    @Inject(method = "tickLevelQueue", at = @At("RETURN"), require = 0)
    private void observable$endTickLevelQueue(CallbackInfo ci) {
        AE2GridProfiler.endTickManagerSection(this, AE2GridProfiler.TICK_SECTION_LEVEL_QUEUE);
    }

    @Inject(method = "tickQueue", at = @At("HEAD"), require = 0)
    private void observable$beginTickQueue(CallbackInfo ci) {
        AE2GridProfiler.beginTickManagerSection(this, AE2GridProfiler.TICK_SECTION_QUEUE);
        AE2GridProfiler.beginTickQueueDetail(this);
    }

    /**
     * v10 queue phase boundary: a queue head has been selected. Closing the
     * previous branch here keeps requeue/sleep time separate from the next
     * iteration's due check.
     */
    @Inject(
            method = "tickQueue",
            at = @At(value = "INVOKE", target = "Ljava/util/PriorityQueue;peek()Ljava/lang/Object;"),
            require = 0)
    private void observable$tickQueuePeek(CallbackInfo ci) {
        AE2GridProfiler.tickQueueHeadCheck(this);
    }

    /** The head was due; PriorityQueue.poll and tracker preparation follow. */
    @Inject(
            method = "tickQueue",
            at = @At(value = "INVOKE", target = "Ljava/util/PriorityQueue;poll()Ljava/lang/Object;"),
            require = 0)
    private void observable$tickQueuePoll(CallbackInfo ci) {
        AE2GridProfiler.tickQueuePoll(this);
    }

    /** Device execution begins after dequeue/diff/node preparation. */
    @Inject(
            method = "tickQueue",
            at = @At(
                    value = "INVOKE",
                    target = "Lappeng/me/service/TickManagerService;unsafeTickingRequest(Lappeng/me/service/helpers/TickTracker;I)Lappeng/api/networking/ticking/TickRateModulation;"),
            require = 0)
    private void observable$tickQueueBeforeDevice(CallbackInfo ci) {
        AE2GridProfiler.tickQueueBeforeDevice(this);
    }

    /**
     * setLastTick is the first stable invocation after unsafeTickingRequest in
     * AE2 1.20.x. Start the post-device rate/modulation phase here.
     */
    @Inject(
            method = "tickQueue",
            at = @At(
                    value = "INVOKE",
                    target = "Lappeng/me/service/helpers/TickTracker;setLastTick(J)V"),
            require = 0)
    private void observable$tickQueueAfterDevice(CallbackInfo ci) {
        AE2GridProfiler.tickQueueAfterDevice(this);
    }

    /** SLEEP modulation enters the sleep/remove-from-queue branch. */
    @Inject(
            method = "tickQueue",
            at = @At(
                    value = "INVOKE",
                    target = "Lappeng/me/service/TickManagerService;sleepDevice(Lappeng/api/networking/IGridNode;)Z"),
            require = 0)
    private void observable$tickQueueSleepBranch(CallbackInfo ci) {
        AE2GridProfiler.tickQueueBranch(this, AE2GridProfiler.TICK_BRANCH_SLEEP);
    }

    /** Non-sleep modulation enters the awake-check/requeue branch. */
    @Inject(
            method = "tickQueue",
            at = @At(value = "INVOKE", target = "Ljava/util/Map;containsKey(Ljava/lang/Object;)Z"),
            require = 0)
    private void observable$tickQueueRequeueBranch(CallbackInfo ci) {
        AE2GridProfiler.tickQueueBranch(this, AE2GridProfiler.TICK_BRANCH_REQUEUE);
    }

    /** The node is still awake; PriorityQueue.add performs the heap reinsert. */
    @Inject(
            method = "tickQueue",
            at = @At(value = "INVOKE", target = "Ljava/util/PriorityQueue;add(Ljava/lang/Object;)Z"),
            require = 0)
    private void observable$tickQueueReinsert(CallbackInfo ci) {
        AE2GridProfiler.tickQueueReinsert(this);
    }

    @Inject(method = "tickQueue", at = @At("RETURN"), require = 0)
    private void observable$endTickQueue(CallbackInfo ci) {
        AE2GridProfiler.finishTickQueueDetail(this);
        AE2GridProfiler.endTickManagerSection(this, AE2GridProfiler.TICK_SECTION_QUEUE);
    }

    @Inject(method = "sleepDevice", at = @At("HEAD"), require = 0)
    private void observable$beginSleepDevice(CallbackInfoReturnable<Boolean> cir) {
        AE2GridProfiler.beginTickManagerControl(this, AE2GridProfiler.TICK_CONTROL_SLEEP);
    }

    @Inject(method = "sleepDevice", at = @At("RETURN"), require = 0)
    private void observable$endSleepDevice(CallbackInfoReturnable<Boolean> cir) {
        AE2GridProfiler.endTickManagerControl(this, AE2GridProfiler.TICK_CONTROL_SLEEP);
    }

    @Inject(method = "wakeDevice", at = @At("HEAD"), require = 0)
    private void observable$beginWakeDevice(CallbackInfoReturnable<Boolean> cir) {
        AE2GridProfiler.beginTickManagerControl(this, AE2GridProfiler.TICK_CONTROL_WAKE);
    }

    @Inject(method = "wakeDevice", at = @At("RETURN"), require = 0)
    private void observable$endWakeDevice(CallbackInfoReturnable<Boolean> cir) {
        AE2GridProfiler.endTickManagerControl(this, AE2GridProfiler.TICK_CONTROL_WAKE);
    }

    @Inject(method = "alertDevice", at = @At("HEAD"), require = 0)
    private void observable$beginAlertDevice(CallbackInfoReturnable<Boolean> cir) {
        AE2GridProfiler.beginTickManagerControl(this, AE2GridProfiler.TICK_CONTROL_ALERT);
    }

    @Inject(method = "alertDevice", at = @At("RETURN"), require = 0)
    private void observable$endAlertDevice(CallbackInfoReturnable<Boolean> cir) {
        AE2GridProfiler.endTickManagerControl(this, AE2GridProfiler.TICK_CONTROL_ALERT);
    }
}
