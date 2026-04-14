using System;

namespace Hedera.Hashgraph.Proto.Services
{
    public static class DurationExtensions
    {
        public static TimeSpan ToTimeSpan(this Duration duration)
        {
            return TimeSpan.FromSeconds(duration.Seconds);
        }
        public static Duration ToProtoDuration(this TimeSpan timeSpan)
        {
            return new Duration { Seconds = (long)timeSpan.TotalSeconds };
        }
    }
}
