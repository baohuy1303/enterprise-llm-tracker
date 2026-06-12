import { LeaderboardTable } from "@/app/_components/LeaderboardTable";
import { getLeaderboard, listEngineers } from "@/lib/sentinel-api";

export default async function LeaderboardPage() {
  const [engineersRes, leaderboardRes] = await Promise.all([
    listEngineers(),
    getLeaderboard("7d").catch(() => null),
  ]);

  return (
    <LeaderboardTable
      engineers={engineersRes.engineers}
      initialEntries={leaderboardRes?.entries ?? []}
      initialWindow="7d"
    />
  );
}
