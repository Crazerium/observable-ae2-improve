package observable.forge.mixin;

import observable.forge.compat.CompatTiming;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Pseudo;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

/**
 * Registers ME Drives even though DriveBlockEntity itself does not need to be
 * a vanilla ticking block entity. This lets an idle drive appear as 0 us/t in
 * Observable instead of disappearing from the world overlay entirely.
 */
@Pseudo
@Mixin(targets = "appeng.blockentity.storage.DriveBlockEntity", remap = false)
public abstract class AE2DriveBlockEntityMixin {
    @Inject(method = "<init>*", at = @At("RETURN"), require = 0)
    private void observable$registerDriveAfterConstruction(CallbackInfo ci) {
        CompatTiming.registerAE2Drive(this);
    }

    // Some AE2 builds finish attaching the level/grid after construction.
    // Re-registering is idempotent and also materializes the drive immediately
    // if a profiling session is already running when its chunk is loaded.
    @Inject(method = "onReady", at = @At("RETURN"), require = 0)
    private void observable$registerDriveWhenReady(CallbackInfo ci) {
        CompatTiming.registerAE2Drive(this);
    }

    // AE2 15.5.0 creates DriveWatcher only inside updateStateForSlot().
    // Open a short-lived owner context before the constructor runs so the
    // DriveWatcher mixin can bind the exact newly-constructed identity to this
    // DriveBlockEntity. The previous post-slot array capture remains as a
    // second exact owner-side check.
    @Inject(method = "updateStateForSlot(I)D", at = @At("HEAD"), require = 0)
    private void observable$beginWatcherOwnerContext(
            int slot, CallbackInfoReturnable<Double> cir) {
        CompatTiming.beginAE2DriveWatcherOwnerContext(this);
    }

    @Inject(method = "updateStateForSlot(I)D", at = @At("RETURN"), require = 0)
    private void observable$recordWatcherOwnerAfterSlotRefresh(
            int slot, CallbackInfoReturnable<Double> cir) {
        try {
            CompatTiming.recordAE2DriveWatcherSlotOwner(this, slot);
        } finally {
            CompatTiming.endAE2DriveWatcherOwnerContext(this);
        }
    }
}
