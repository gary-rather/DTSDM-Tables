import java.util.Optional;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

public class MyTestWatcher implements TestWatcher {
    @Override
    public void testAborted(ExtensionContext extensionContext, Throwable throwable) {
        // do something
        System.out.println("MyWatcher testAborted");
    }

    @Override
    public void testDisabled(ExtensionContext extensionContext, Optional<String> optional) {
        // do something
        String name = extensionContext.getDisplayName();
        System.out.println("MyWatcher testDisabled "+ name);
    }

    @Override
    public void testFailed(ExtensionContext extensionContext, Throwable throwable) {
        // do something
        String name = extensionContext.getDisplayName();
        System.out.println("MyWatcher testFailed " + name);


    }

    @Override
    public void testSuccessful(ExtensionContext extensionContext) {
        // do something
        String name = extensionContext.getDisplayName();
        System.out.println("MyWatcher testSuccessful " + name);
    }
}