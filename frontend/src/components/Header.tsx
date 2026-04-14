"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { Flame } from 'lucide-react';

import { SearchInput } from "./SearchInput";

const NAV_ITEMS = [
  { href: "/", label: "Home" },
  { href: "/feed", label: "Feed" },
  { href: "/agencies", label: "Agencies" },
];

export function Header() {
  const pathname = usePathname();

  return (
    <header className="border-b border-[var(--border)] bg-[var(--surface)]/80 backdrop-blur-xl sticky top-0 z-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center gap-4 h-14">
          {/* Logo */}
          <Link href="/" className="flex items-center gap-1.5 flex-shrink-0">
            <Flame size={22} className="text-[var(--accent-primary)]" />
            <span className="text-lg font-bold gradient-text font-serif hidden sm:inline">
              Spicy Regs
            </span>
          </Link>

          {/* Search */}
          <SearchInput className="flex-1 max-w-md" />

          {/* Navigation */}
          <nav className="flex items-center gap-0.5">
            {NAV_ITEMS.map((item) => {
              const isActive =
                (item.href === '/' ? pathname === '/' : pathname === item.href) ||
                (item.href === '/agencies' && pathname?.startsWith('/sr'));
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all ${
                    isActive
                      ? "bg-[var(--accent-primary)]/10 text-[var(--accent-primary)]"
                      : "text-[var(--muted)] hover:text-[var(--foreground)] hover:bg-[var(--surface-elevated)]"
                  }`}
                >
                  {item.label}
                </Link>
              );
            })}
          </nav>
        </div>
      </div>
    </header>
  );
}
