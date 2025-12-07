"use client";

import { createClient } from "@/utils/supabase/client";
import { useRouter } from "next/navigation";

export function SignOutButton() {
  const router = useRouter();
  const supabase = createClient();

  const handleSignOut = async () => {
    await supabase.auth.signOut();
    router.refresh();
  };

  return (
    <button
      onClick={handleSignOut}
      className="bg-transparent hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-700 dark:text-gray-200 font-semibold py-2 px-4 border border-gray-300 dark:border-gray-600 rounded shadow-sm text-sm transition-colors"
    >
      Sign Out
    </button>
  );
}
