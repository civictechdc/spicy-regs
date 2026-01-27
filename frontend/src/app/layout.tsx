import type { Metadata } from "next";
import { DuckDBProvider } from "@/lib/duckdb/context";

import "./globals.css";
import { SpeedInsights } from "@vercel/speed-insights/next"
import { Analytics } from "@vercel/analytics/next"

export const metadata: Metadata = {
  title: "spicy regs",
  description: "spicy regs is an open source civic tech project that creates a platform using regulations.gov data for consumers to extend",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className={"antialiased"}>
        <DuckDBProvider>
          {children}
        </DuckDBProvider>
        <SpeedInsights />
        <Analytics />
      </body>
    </html>
  );
}

