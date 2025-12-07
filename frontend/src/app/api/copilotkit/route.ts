import {
  CopilotRuntime,
  OpenAIAdapter,
  copilotRuntimeNextJSAppRouterEndpoint,
} from "@copilotkit/runtime";
import { NextRequest } from "next/server";
import { OpenAI } from "openai";
import { getAgencies } from "@/lib/mirrulations/service";
import { getData } from "@/lib/db/service";
import { RegulationsDataTypes } from "@/lib/db/models";

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY || "dummy-key-for-build",
});

const serviceAdapter = new OpenAIAdapter({ 
  openai,
  model: "gpt-5-nano",
});

const runtime = new CopilotRuntime({
  actions: [
    {
      name: "get_agencies",
      description: "Get a list of all agencies from the Mirrulations S3 bucket",
      handler: async () => {
        return await getAgencies();
      },
    },
    {
      name: "get_data",
      description: "Get a list of all data for a given data type for an agency from the database.",
      parameters: [
        {
          name: "data_type",
          type: "string",
          description: "One of 'dockets', 'comments', or 'documents'",
          required: true,
          enum: Object.values(RegulationsDataTypes),
        },
        {
          name: "agency_code",
          type: "string",
          description: "The agency code (e.g. 'NASA')",
          required: true,
        },
        {
          name: "docket_id",
          type: "string",
          description: "The docket ID",
          required: false,
        },
      ],
      handler: async ({ data_type, agency_code, docket_id }) => {
        return await getData(data_type as RegulationsDataTypes, agency_code, docket_id);
      },
    },
  ],
});

export const POST = async (req: NextRequest) => {
  const { handleRequest } = copilotRuntimeNextJSAppRouterEndpoint({
    runtime,
    serviceAdapter,
    endpoint: "/api/copilotkit",
  });

  return handleRequest(req);
};