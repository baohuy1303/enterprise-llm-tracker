import { AddEngineerForm } from "@/app/_components/AddEngineerForm";

export default function NewEngineerPage() {
  return (
    <div className="max-w-xl space-y-4">
      <div>
        <h1 className="text-xl font-semibold">Add engineer</h1>
        <p className="text-sm text-zinc-500 dark:text-zinc-400">
          Registers a new engineer in Sentinel&apos;s tracking registry.
        </p>
      </div>
      <AddEngineerForm />
    </div>
  );
}
