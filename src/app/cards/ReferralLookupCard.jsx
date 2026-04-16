import React, { useMemo, useState } from "react";
import {
  hubspot,
  Alert,
  Box,
  Button,
  DescriptionList,
  DescriptionListItem,
  Divider,
  Flex,
  LoadingButton,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  Text
} from "@hubspot/ui-extensions";
import { useCrmProperties } from "@hubspot/ui-extensions/crm";

hubspot.extend(({ context, actions }) => (
  <ReferralLookupCard context={context} actions={actions} />
));

const CRM_PROPERTIES = [
  "firstname",
  "lastname",
  "specialty_required",
  "what_insurance_plan_and_company_are_you_using",
  "zip"
];

const REQUIRED_FIELDS = [
  { key: "specialty_required", label: "Specialty needed" },
  {
    key: "what_insurance_plan_and_company_are_you_using",
    label: "Insurance"
  },
  { key: "zip", label: "ZIP" }
];

function ReferralLookupCard({ context, actions }) {
  const [loading, setLoading] = useState(false);
  const [results, setResults] = useState([]);
  const [createdCount, setCreatedCount] = useState(0);
  const [error, setError] = useState("");
  const [lastPatientName, setLastPatientName] = useState("");

  const contactId =
    context?.crm?.objectId ||
    context?.objectId ||
    context?.recordId ||
    "";

  const {
    properties = {},
    isLoading: propertiesLoading,
    error: propertiesError
  } = useCrmProperties(CRM_PROPERTIES);

  const specialtyNeeded = properties?.specialty_required || "";
  const insurance =
    properties?.what_insurance_plan_and_company_are_you_using || "";
  const zip = properties?.zip || "";
  const firstname = properties?.firstname || "";
  const lastname = properties?.lastname || "";

  const patientName = [firstname, lastname].filter(Boolean).join(" ").trim();
  const missingFields = useMemo(() => {
    return REQUIRED_FIELDS.filter(({ key }) => {
      const value = properties?.[key];
      return !value || String(value).trim() === "";
    });
  }, [properties]);

  const isDisabled =
    loading || propertiesLoading || !contactId || missingFields.length > 0;

  const handleGetReferrals = async () => {
    setLoading(true);
    setError("");
    setResults([]);
    setCreatedCount(0);

    try {
      const result = await hubspot.serverless("hoi_referral_generate", {
        parameters: {
          contactId
        },
        propertiesToSend: [
          "firstname",
          "lastname",
          "specialty_required",
          "what_insurance_plan_and_company_are_you_using",
          "zip"
        ]
      });

      const response = result?.body || result;

      if (response?.status === "error") {
        throw new Error(response.message || "Unknown error");
      }

      if (response?.message && (response?.results || []).length === 0) {
        setError(response.message);
      }

      setResults(response?.results || []);
      setCreatedCount(response?.createdCount || 0);
      setLastPatientName(response?.contactName || patientName || "Contact");
    } catch (err) {
      setError(err.message || "Something went wrong while generating referrals.");
    } finally {
      setLoading(false);
    }
  };

  const handleReset = () => {
    setResults([]);
    setCreatedCount(0);
    setError("");
    setLastPatientName("");
  };

  return (
    <Box>
      <Flex direction="column" gap="small">
        <Text format={{ fontWeight: "bold" }}>
          Physician Referral Lookup
        </Text>

        <DescriptionList>
          <DescriptionListItem label="Specialty needed">
            {specialtyNeeded || "—"}
          </DescriptionListItem>
          <DescriptionListItem label="Insurance">
            {insurance || "—"}
          </DescriptionListItem>
          <DescriptionListItem label="ZIP">
            {zip || "—"}
          </DescriptionListItem>
        </DescriptionList>

        {!contactId && (
          <Alert title="Missing contact context" variant="warning">
            This card must be opened from a Contact record.
          </Alert>
        )}

        {propertiesError && (
          <Alert title="Unable to load record properties" variant="warning">
            The card could not load the current record values.
          </Alert>
        )}

        {missingFields.length > 0 && (
          <Alert title="Missing required fields" variant="warning">
            Please fill in: {missingFields.map((f) => f.label).join(", ")}
          </Alert>
        )}

        {error && (
          <Alert title="Info" variant="warning">
            {error}
          </Alert>
        )}

        {!error && results.length === 0 && missingFields.length === 0 && (
          <Text>
            Click Get Referrals to read the contact criteria, query HubDB,
            randomize matching physicians, create referral records, and display up
            to 3 results.
          </Text>
        )}

        {propertiesLoading && (
          <Text>Loading current patient data...</Text>
        )}

        <Flex gap="small">
          <LoadingButton
            onClick={handleGetReferrals}
            loading={loading}
            disabled={isDisabled}
          >
            Get Referrals
          </LoadingButton>

          <Button variant="secondary" onClick={handleReset} disabled={loading}>
            New Search
          </Button>
        </Flex>

        {results.length > 0 && (
          <>
            <Divider />

            <Alert title="Success" variant="success">
              {createdCount} referral record{createdCount === 1 ? "" : "s"} created for{" "}
              {lastPatientName || "Contact"}.
            </Alert>

            <Table bordered paginated={false}>
              <TableHead>
                <TableRow>
                  <TableHeader>Physician Name</TableHeader>
                  <TableHeader>Phone</TableHeader>
                  <TableHeader>Fax</TableHeader>
                  <TableHeader>Address</TableHeader>
                </TableRow>
              </TableHead>
              <TableBody>
                {results.map((item, index) => (
                  <TableRow key={`${item.hubdbRowId || "row"}-${index}`}>
                    <TableCell>{item.physicianName || "—"}</TableCell>
                    <TableCell>{item.phone || "—"}</TableCell>
                    <TableCell>{item.fax || "—"}</TableCell>
                    <TableCell>{item.address || "—"}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </>
        )}
      </Flex>
    </Box>
  );
}
