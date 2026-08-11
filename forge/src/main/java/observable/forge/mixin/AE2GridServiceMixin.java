package observable.forge.mixin;

import appeng.api.networking.IGridServiceProvider;
import net.minecraft.world.level.Level;
import observable.forge.compat.AE2GridProfiler;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Pseudo;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.Redirect;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

/**
 * v8 profiles AE2 grid services at the Grid dispatch boundary.
 *
 * This target is intentionally only appeng.me.Grid. We no longer transform
 * individual service implementation classes. Redirecting the interface calls
 * made by Grid catches every registered IGridServiceProvider, including addon
 * services and GTO-fork implementations whose concrete class differs from
 * upstream AE2.
 */
@Pseudo
@Mixin(targets = "appeng.me.Grid", remap = false)
public abstract class AE2GridServiceMixin {
    @Inject(method = "onServerStartTick", at = @At("HEAD"), require = 0)
    private void observable$gridServerStartBegin(CallbackInfo ci) {
        AE2GridProfiler.beginGridLifecycle(this, AE2GridProfiler.PHASE_SERVER_START);
    }

    @Inject(method = "onServerStartTick", at = @At("RETURN"), require = 0)
    private void observable$gridServerStartEnd(CallbackInfo ci) {
        AE2GridProfiler.endGridLifecycle(this, AE2GridProfiler.PHASE_SERVER_START);
    }

    @Inject(method = "onLevelStartTick", at = @At("HEAD"), require = 0)
    private void observable$gridLevelStartBegin(Level level, CallbackInfo ci) {
        AE2GridProfiler.beginGridLifecycle(this, AE2GridProfiler.PHASE_LEVEL_START);
    }

    @Inject(method = "onLevelStartTick", at = @At("RETURN"), require = 0)
    private void observable$gridLevelStartEnd(Level level, CallbackInfo ci) {
        AE2GridProfiler.endGridLifecycle(this, AE2GridProfiler.PHASE_LEVEL_START);
    }

    @Inject(method = "onLevelEndTick", at = @At("HEAD"), require = 0)
    private void observable$gridLevelEndBegin(Level level, CallbackInfo ci) {
        AE2GridProfiler.beginGridLifecycle(this, AE2GridProfiler.PHASE_LEVEL_END);
    }

    @Inject(method = "onLevelEndTick", at = @At("RETURN"), require = 0)
    private void observable$gridLevelEndEnd(Level level, CallbackInfo ci) {
        AE2GridProfiler.endGridLifecycle(this, AE2GridProfiler.PHASE_LEVEL_END);
    }

    @Inject(method = "onServerEndTick", at = @At("HEAD"), require = 0)
    private void observable$gridServerEndBegin(CallbackInfo ci) {
        AE2GridProfiler.beginGridLifecycle(this, AE2GridProfiler.PHASE_SERVER_END);
    }

    @Inject(method = "onServerEndTick", at = @At("RETURN"), require = 0)
    private void observable$gridServerEndEnd(CallbackInfo ci) {
        AE2GridProfiler.endGridLifecycle(this, AE2GridProfiler.PHASE_SERVER_END);
    }

    @Redirect(
            method = "onServerStartTick",
            at = @At(
                    value = "INVOKE",
                    target = "Lappeng/api/networking/IGridServiceProvider;onServerStartTick()V",
                    remap = false
            ),
            require = 0
    )
    private void observable$profileServerStartService(IGridServiceProvider service) {
        Object token = AE2GridProfiler.beginService(this, service, AE2GridProfiler.PHASE_SERVER_START);
        try {
            service.onServerStartTick();
        } finally {
            AE2GridProfiler.endService(token);
        }
    }

    @Redirect(
            method = "onLevelStartTick",
            at = @At(
                    value = "INVOKE",
                    target = "Lappeng/api/networking/IGridServiceProvider;onLevelStartTick(Lnet/minecraft/world/level/Level;)V",
                    remap = false
            ),
            require = 0
    )
    private void observable$profileLevelStartService(IGridServiceProvider service, Level level) {
        Object token = AE2GridProfiler.beginService(this, service, AE2GridProfiler.PHASE_LEVEL_START, level);
        try {
            service.onLevelStartTick(level);
        } finally {
            AE2GridProfiler.endService(token);
        }
    }

    @Redirect(
            method = "onLevelEndTick",
            at = @At(
                    value = "INVOKE",
                    target = "Lappeng/api/networking/IGridServiceProvider;onLevelEndTick(Lnet/minecraft/world/level/Level;)V",
                    remap = false
            ),
            require = 0
    )
    private void observable$profileLevelEndService(IGridServiceProvider service, Level level) {
        Object token = AE2GridProfiler.beginService(this, service, AE2GridProfiler.PHASE_LEVEL_END, level);
        try {
            service.onLevelEndTick(level);
        } finally {
            AE2GridProfiler.endService(token);
        }
    }

    @Redirect(
            method = "onServerEndTick",
            at = @At(
                    value = "INVOKE",
                    target = "Lappeng/api/networking/IGridServiceProvider;onServerEndTick()V",
                    remap = false
            ),
            require = 0
    )
    private void observable$profileServerEndService(IGridServiceProvider service) {
        Object token = AE2GridProfiler.beginService(this, service, AE2GridProfiler.PHASE_SERVER_END);
        try {
            service.onServerEndTick();
        } finally {
            AE2GridProfiler.endService(token);
        }
    }
}
