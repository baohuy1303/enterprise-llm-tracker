import { EngineerDetail } from "@/app/_components/EngineerDetail";
import { Card } from "@/app/components/Card";
import { getEngineer, SentinelApiError } from "@/lib/sentinel-api";

type RouteParams = {
  params: Promise<{ email: string }>;
};

export default async function EngineerDetailPage({ params }: RouteParams) {
  const { email: rawEmail } = await params;
  // Next does not decode `@`/`%40` in dynamic segments, so decode once here
  // before re-encoding for the upstream API call.
  const email = decodeURIComponent(rawEmail);

  try {
    const detail = await getEngineer(email);
    return <EngineerDetail email={email} initialDetail={detail} />;
  } catch (err) {
    if (err instanceof SentinelApiError && err.status === 404) {
      return (
        <Card>
          <p className="text-sm text-zinc-600 dark:text-zinc-400">
            No engineer found for <span className="font-mono">{email}</span>.
          </p>
        </Card>
      );
    }
    throw err;
  }
}
