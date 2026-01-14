import type { Metadata } from "next";
import { MotherDuckClientProvider } from "@/lib/motherduck/context/motherduckClientContext";

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
        <MotherDuckClientProvider database="spicy-regs">
          {children}
        </MotherDuckClientProvider>
        <SpeedInsights />
        <Analytics />
      </body>
    </html>
  );
}
