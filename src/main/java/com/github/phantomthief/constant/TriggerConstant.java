package com.github.phantomthief.constant;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author st4rlight <st4rlight@163.com>
 * Created on 2023-05-14
 */
public class TriggerConstant {

    private TriggerConstant() {
        throw new UnsupportedOperationException();
    }

    public static final long DEFAULT_NEXT_TRIGGER_PERIOD = TimeUnit.SECONDS.toMillis(1);

    public static final Duration DEFAULT_LINGER = Duration.ofSeconds(1);
}
