package observable.forge;

import dev.architectury.platform.forge.EventBuses;
import net.minecraftforge.common.MinecraftForge;
import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.fml.event.lifecycle.FMLClientSetupEvent;
import net.minecraftforge.fml.javafmlmod.FMLJavaModLoadingContext;
import observable.Observable;
import observable.forge.compat.AE2GridRuntimeBridge;
import observable.forge.compat.GTTaskRuntimeBridge;
import observable.server.ModLoader;
import observable.server.Remapper;

import static observable.Observable.init;

@Mod(Observable.MOD_ID)
public class ObservableForge {
    public ObservableForge() {
        Remapper.modLoader = ModLoader.FORGE;
        // Submit our event bus to let architectury register our content on the right time
        EventBuses.registerModEventBus(Observable.MOD_ID, FMLJavaModLoadingContext.get().getModEventBus());
        FMLJavaModLoadingContext.get().getModEventBus().addListener(this::onClientInit);

        // GTOLib rejects bytecode transformations of protected GTCEu classes.
        // This listener profiles GT task entries at runtime without any GT mixin.
        GTTaskRuntimeBridge.register();

        // AE2 grid services are network-wide and cannot be attributed to one
        // physical block. This bridge resets/publishes the per-grid collector.
        AE2GridRuntimeBridge.register();

        init();
    }

    public void onClientInit(FMLClientSetupEvent ev) {
        Observable.clientInit();
        MinecraftForge.EVENT_BUS.register(ForgeClientHooks.INSTANCE);
    }
}
