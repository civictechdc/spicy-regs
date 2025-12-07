
import Link from "next/link";
import { createClient } from "@/utils/supabase/server";
import { redirect } from "next/navigation";

export default async function LandingPage() {
  const supabase = await createClient();

  const {
    data: { user },
  } = await supabase.auth.getUser();

  if (user) {
    redirect("/dashboard");
  }

  return (
    <div className="flex min-h-screen flex-col items-center justify-center bg-gray-900 py-12 px-4 sm:px-6 lg:px-8">
      <div className="max-w-md w-full space-y-8 text-center">
        <div>
          <h1 className="mt-6 text-4xl font-extrabold text-white tracking-tight">
            Spicy Regs
          </h1>
          <p className="mt-2 text-lg text-gray-300">
            Spicy Regs goal is to build an open, contributor-friendly platform for exploring and analyzing regulations.gov data, usable by both technical and non-technical users. The platform should enable rapid prototyping, reproducible analysis, and modular app extensions.
          </p>
        </div>
        <div className="mt-8">
          <Link
            href="/login"
            className="w-full flex items-center justify-center px-8 py-3 border border-transparent text-base font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700 md:py-4 md:text-lg md:px-10 transition-colors"
          >
            Sign in
          </Link>
        </div>
        <div className="mt-6 text-sm">
           <p className="text-gray-400">
            Join our <a href="https://civictechdc.slack.com/archives/C09H576E6LU" className="font-medium text-indigo-400 hover:text-indigo-300 transition-colors" target="_blank" rel="noopener noreferrer">slack channel</a>!
          </p>
        </div>
      </div>
    </div>
  );
}
