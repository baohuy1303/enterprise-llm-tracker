import { HealthDashboard, type HealthSummary } from "@/app/_components/HealthDashboard";
import { getHealthz, getReadyz, getRecentUsage, listEngineers } from "@/lib/sentinel-api";

export default async function HealthPage() {
  const [healthy, ready, engineersRes, recentUsage] = await Promise.all([
    getHealthz(),
    getReadyz().catch(() => null),
    listEngineers(),
    getRecentUsage(50).catch(() => ({ events: [], count: 0 })),
  ]);

  const initialHealth: HealthSummary = ready ? { healthy, ...ready } : { healthy };

  return (
    <HealthDashboard
      initialHealth={initialHealth}
      initialEngineers={engineersRes.engineers}
      initialEvents={recentUsage.events}
    />
  );
}
