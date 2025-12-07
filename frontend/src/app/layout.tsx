import type { Metadata } from "next";

import { CopilotKit } from "@copilotkit/react-core";
import "./globals.css";
import "@copilotkit/react-ui/styles.css";

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
        <CopilotKit runtimeUrl="/api/copilotkit" publicApiKey="ck_pub_0afe2c88b7ff46abe7dfdeef22626f17">
          {children}
        </CopilotKit>
      </body>
    </html>
  );
}
