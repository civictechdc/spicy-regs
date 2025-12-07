import { tool } from "@langchain/core/tools";
import { z } from "zod";
import { getAgencies } from "../lib/mirrulations/service";
import { getData } from "../lib/db/service";
import { RegulationsDataTypes } from "../lib/db/models";

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

const getDataSchema = z.object({
  data_type: z.enum([
    RegulationsDataTypes.Dockets,
    RegulationsDataTypes.Comments,
    RegulationsDataTypes.Documents,
  ]),
  agency_code: z.string().describe("The agency code (e.g. 'NASA')"),
  docket_id: z.string().optional().describe("The docket ID"),
});

export const getDataTool = tool(
  async (input) => {
    const { data_type, agency_code, docket_id } = input as z.infer<typeof getDataSchema>;
    return await getData(data_type as RegulationsDataTypes, agency_code, docket_id);
  },
  {
    name: "get_data",
    description: "Get a list of all data for a given data type for an agency from the database. If docket_id is provided, return data for that docket id.",
    schema: getDataSchema,
  }
);

export const tools = [getAgenciesTool, getDataTool];
export const toolsByName = {
  get_agencies: getAgenciesTool,
  get_data: getDataTool,
};
