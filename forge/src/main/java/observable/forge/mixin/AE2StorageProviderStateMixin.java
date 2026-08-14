package observable.forge.mixin;

import observable.forge.compat.CompatTiming;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Pseudo;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/**
 * Captures AE2's authoritative storage-provider mount relation without adding
 * a compile-time AE2 storage API dependency. ProviderState.mount() has no
 * arguments and, at RETURN, its private inventories set contains exactly the
 * MEStorage instances that the provider just published to NetworkStorage.
 */
@Pseudo
@Mixin(targets = "appeng.me.service.StorageService$ProviderState", remap = false)
public abstract class AE2StorageProviderStateMixin {
    @Inject(method = "mount()V", at = @At("RETURN"), require = 0)
    private void observable$recordMountedStorageOwners(CallbackInfo ci) {
        CompatTiming.recordAE2StorageProviderMount(this);
    }
}
