import { tool } from "@langchain/core/tools";
import { z } from "zod";
import { getAgencies, getDockets } from "../lib/mirrulations/service"; // Added getDockets export earlier
import { getData } from "../lib/db/service";
import { RegulationsDataTypes } from "../lib/db/models";

// We need to ensure getAgencies and other imported functions are consistent.
// I exported getAgencies and getDockets in lib/mirrulations/service.ts.
// I exported getData in lib/db/service.ts.

export const getAgenciesTool = tool(
  async () => {
    return await getAgencies();
  },
  {
    name: "get_agencies",
    description: "Get a list of all agencies from the Mirrulations S3 bucket",
    schema: z.object({}),
  }
);

export const getDataTool = tool(
  async ({ data_type, agency_code, docket_id }) => {
    // Map string to enum
    // Runtime validation ensures data_type matches enum values if we construct schema correctly
    // or we cast/validate inside.
    return await getData(data_type as RegulationsDataTypes, agency_code, docket_id);
  },
  {
    name: "get_data",
    description: "Get a list of all data for a given data type for an agency from the database. If docket_id is provided, return data for that docket id.",
    schema: z.object({
      data_type: z.enum([
        RegulationsDataTypes.Dockets,
        RegulationsDataTypes.Comments,
        RegulationsDataTypes.Documents,
      ]),
      agency_code: z.string().describe("The agency code (e.g. 'NASA')"),
      docket_id: z.string().optional().describe("The docket ID"),
    }),
  }
);

export const tools = [getAgenciesTool, getDataTool];
export const toolsByName = {
  get_agencies: getAgenciesTool,
  get_data: getDataTool,
};
