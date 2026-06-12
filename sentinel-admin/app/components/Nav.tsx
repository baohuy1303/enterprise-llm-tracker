"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const LINKS = [
  { href: "/", label: "Engineers" },
  { href: "/engineers/new", label: "Add Engineer" },
  { href: "/leaderboard", label: "Leaderboard" },
  { href: "/health", label: "Health" },
];

export function Nav() {
  const pathname = usePathname();

  return (
    <header className="border-b border-zinc-200 bg-white dark:border-zinc-800 dark:bg-zinc-950">
      <div className="mx-auto flex h-14 max-w-6xl items-center gap-6 px-4 sm:px-6">
        <span className="text-lg font-semibold tracking-tight">Sentinel</span>
        <nav className="flex gap-4 text-sm">
          {LINKS.map((link) => {
            const active = pathname === link.href;
            return (
              <Link
                key={link.href}
                href={link.href}
                aria-current={active ? "page" : undefined}
                className={
                  active
                    ? "font-medium text-foreground"
                    : "text-zinc-500 hover:text-foreground dark:text-zinc-400"
                }
              >
                {link.label}
              </Link>
            );
          })}
        </nav>
      </div>
    </header>
  );
}
