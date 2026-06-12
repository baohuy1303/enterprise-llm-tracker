import { getLeaderboard, listEngineers } from "@/lib/sentinel-api";
import { EngineerTable } from "./_components/EngineerTable";

export default async function Home() {
  const [engineersRes, leaderboardRes] = await Promise.all([
    listEngineers(),
    getLeaderboard("7d").catch(() => null),
  ]);

  return (
    <EngineerTable
      initialEngineers={engineersRes.engineers}
      initialLeaderboard={leaderboardRes?.entries ?? []}
    />
  );
}
