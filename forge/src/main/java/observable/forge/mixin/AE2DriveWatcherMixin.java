package observable.forge.mixin;

import observable.forge.compat.CompatTiming;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Pseudo;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

/**
 * Profiles storage-cell work performed through AE2's DriveWatcher wrapper and
 * attributes it back to the owning DriveBlockEntity. Target method arguments
 * are intentionally not captured, keeping this mixin tolerant of minor API
 * signature changes while still observing the stable method names.
 */
@Pseudo
@Mixin(targets = "appeng.me.storage.DriveWatcher", remap = false)
public abstract class AE2DriveWatcherMixin {
    @Inject(method = "insert", at = @At("HEAD"), require = 0)
    private void observable$beginInsert(CallbackInfoReturnable<Long> cir) {
        CompatTiming.beginAE2DriveOperation(this, "drive.insert");
    }

    @Inject(method = "insert", at = @At("RETURN"), require = 0)
    private void observable$endInsert(CallbackInfoReturnable<Long> cir) {
        CompatTiming.endAE2DriveOperation();
    }

    @Inject(method = "extract", at = @At("HEAD"), require = 0)
    private void observable$beginExtract(CallbackInfoReturnable<Long> cir) {
        CompatTiming.beginAE2DriveOperation(this, "drive.extract");
    }

    @Inject(method = "extract", at = @At("RETURN"), require = 0)
    private void observable$endExtract(CallbackInfoReturnable<Long> cir) {
        CompatTiming.endAE2DriveOperation();
    }

    @Inject(method = "isPreferredStorageFor", at = @At("HEAD"), require = 0)
    private void observable$beginPreferredStorageCheck(CallbackInfoReturnable<Boolean> cir) {
        CompatTiming.beginAE2DriveOperation(this, "drive.preferred");
    }

    @Inject(method = "isPreferredStorageFor", at = @At("RETURN"), require = 0)
    private void observable$endPreferredStorageCheck(CallbackInfoReturnable<Boolean> cir) {
        CompatTiming.endAE2DriveOperation();
    }

    @Inject(
            method = "getAvailableStacks(Lappeng/api/stacks/KeyCounter;)V",
            at = @At("HEAD"),
            require = 0)
    private void observable$beginAvailableStacks(CallbackInfo ci) {
        CompatTiming.beginAE2DriveOperation(this, "drive.availableStacks");
    }

    @Inject(
            method = "getAvailableStacks(Lappeng/api/stacks/KeyCounter;)V",
            at = @At("RETURN"),
            require = 0)
    private void observable$endAvailableStacks(CallbackInfo ci) {
        CompatTiming.endAE2DriveOperation();
    }
}
