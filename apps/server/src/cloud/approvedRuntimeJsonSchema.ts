// Generated from ApprovedRuntimeManifest; contract test detects drift.
export const approvedRuntimeJsonSchema = {
  type: "object",
  properties: {
    format: {
      type: "number",
      enum: [1],
    },
    builds: {
      type: "array",
      items: {
        type: "object",
        properties: {
          version: {
            type: "string",
          },
          platform: {
            type: "string",
            enum: ["linux"],
          },
          arch: {
            type: "string",
          },
          archiveSha256: {
            type: "string",
            pattern: "^[0-9a-f]{64}$",
          },
          executableSha256: {
            type: "string",
            pattern: "^[0-9a-f]{64}$",
          },
          clientTreeSha256: {
            type: "string",
            pattern: "^[0-9a-f]{64}$",
          },
        },
        required: [
          "version",
          "platform",
          "arch",
          "archiveSha256",
          "executableSha256",
          "clientTreeSha256",
        ],
        additionalProperties: true,
      },
    },
  },
  required: ["format", "builds"],
  additionalProperties: true,
} as const;
