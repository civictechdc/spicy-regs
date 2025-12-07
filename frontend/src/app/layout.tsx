import type { Metadata } from "next";
import { MotherDuckClientProvider } from "@/lib/motherduck/context/motherduckClientContext";

import { CopilotKit } from "@copilotkit/react-core";
import "./globals.css";
import "@copilotkit/react-ui/styles.css";
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
          <CopilotKit runtimeUrl="/api/copilotkit" publicApiKey="ck_pub_0afe2c88b7ff46abe7dfdeef22626f17">
            {children}
          </CopilotKit>
        </MotherDuckClientProvider>
        <SpeedInsights />
        <Analytics />
      </body>
    </html>
  );
}
