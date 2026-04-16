const HUBSPOT_BASE_URL = "https://api.hubapi.com";

exports.main = async (context = {}) => {
  try {
    const accessToken = process.env.PRIVATE_APP_ACCESS_TOKEN;
    if (!accessToken) {
      return error("Missing PRIVATE_APP_ACCESS_TOKEN.");
    }

    const contactId = context?.parameters?.contactId;
    if (!contactId) {
      return error("Missing contactId.");
    }

    const config = {
      hubdbTableId: process.env.HUBDB_TABLE_ID || "",
      referralObjectType: process.env.REFERRAL_OBJECT_TYPE || "p_referral",
      contactToReferralAssociationTypeId:
        process.env.CONTACT_TO_REFERRAL_ASSOCIATION_TYPE_ID || "",
      contactToReferralAssociationCategory:
        process.env.CONTACT_TO_REFERRAL_ASSOCIATION_CATEGORY || "USER_DEFINED",
      pmiProperty: process.env.CONTACT_PMI_PROPERTY || "",

      columns: {
        aoe: process.env.HUBDB_COL_AOE || "area_of_expertise_aoe",
        insurance: process.env.HUBDB_COL_INSURANCE || "insurances_accepted",
        zips: parseColumnList(
          process.env.HUBDB_COL_ZIPS ||
            process.env.HUBDB_COL_ZIP ||
            "office_1_address_zip,office_2_address_zip,office_3_address_zip,office_4_address_zip,office_5_address_zip"
        ),
        physicianName: process.env.HUBDB_COL_PHYSICIAN_NAME || "",
        npi: process.env.HUBDB_COL_NPI || "npi",
        phone: process.env.HUBDB_COL_PHONE || "office_1_address_telephone",
        fax: process.env.HUBDB_COL_FAX || "",
        address: process.env.HUBDB_COL_ADDRESS || "office_1_address"
      },

      referralProperties: {
        physicianName:
          process.env.REFERRAL_PROP_PHYSICIAN_NAME || "physician_name",
        physicianNpi: process.env.REFERRAL_PROP_PHYSICIAN_NPI || "physician_npi",
        specialty: process.env.REFERRAL_PROP_SPECIALTY || "specialty",
        location: process.env.REFERRAL_PROP_LOCATION || "location",
        phone: process.env.REFERRAL_PROP_PHONE || "phone",
        referredBy: process.env.REFERRAL_PROP_REFERRED_BY || "referred_by",
        searchInsurance:
          process.env.REFERRAL_PROP_SEARCH_INSURANCE || "search_insurance",
        searchLocation:
          process.env.REFERRAL_PROP_SEARCH_LOCATION || "search_location",
        searchSpecialty:
          process.env.REFERRAL_PROP_SEARCH_SPECIALTY || "search_specialty",
        patientMedicalId:
          process.env.REFERRAL_PROP_PATIENT_MEDICAL_ID || "patient_medical_id",
        hubdbRowId: process.env.REFERRAL_PROP_HUBDB_ROW_ID || "hubdb_row_id",
        referralDate: process.env.REFERRAL_PROP_REFERRAL_DATE || "referral_date"
      }
    };

    if (!config.hubdbTableId) {
      return error("Missing HUBDB_TABLE_ID secret.");
    }

    const sentProps = context?.propertiesToSend || {};
    const firstname = sentProps.firstname || "";
    const lastname = sentProps.lastname || "";
    const contactName = [firstname, lastname].filter(Boolean).join(" ").trim();

    const specialtyNeeded = clean(sentProps.specialty_required);
    const insurance = clean(
      sentProps.what_insurance_plan_and_company_are_you_using
    );
    const zip = clean(sentProps.zip);

    const pmi = config.pmiProperty
      ? await getContactProperty(accessToken, contactId, config.pmiProperty)
      : "";

    const missing = [];
    if (!specialtyNeeded) missing.push("specialty_required");
    if (!insurance) missing.push("what_insurance_plan_and_company_are_you_using");
    if (!zip) missing.push("zip");

    if (missing.length) {
      return {
        status: "error",
        message: `Missing required contact properties: ${missing.join(", ")}`
      };
    }

    const hubdbRows = await getHubDbRows(accessToken, config.hubdbTableId);

    const evaluatedRows = hubdbRows.map((row) => {
      const aoeValue = firstNonEmptyValue([
        readColumn(row, config.columns.aoe),
        readColumn(row, "area_of_expertise_aoe"),
        readColumn(row, "specialty")
      ]);
      const insuranceValue = firstNonEmptyValue([
        readColumn(row, config.columns.insurance),
        readColumn(row, "insurances_accepted")
      ]);
      const zipValues = uniqueValues([
        ...config.columns.zips.map((key) => readColumn(row, key)),
        readColumn(row, "office_1_address_zip"),
        readColumn(row, "office_2_address_zip"),
        readColumn(row, "office_3_address_zip"),
        readColumn(row, "office_4_address_zip"),
        readColumn(row, "office_5_address_zip")
      ]);

      const specialtyMatch = matchesText(aoeValue, specialtyNeeded);
      const insuranceMatch = matchesInsurance(insuranceValue, insurance);
      const zipMatch = matchesAnyZip(zipValues, zip);

      return {
        row,
        aoeValue,
        insuranceValue,
        zipValues,
        specialtyMatch,
        insuranceMatch,
        zipMatch,
        score: [specialtyMatch, insuranceMatch, zipMatch].filter(Boolean).length
      };
    });

    const filtered = evaluatedRows
      .filter((item) => item.specialtyMatch && item.insuranceMatch && item.zipMatch)
      .map((item) => item.row);

    if (!filtered.length) {
      const specialtyMatches = evaluatedRows.filter((item) => item.specialtyMatch).length;
      const insuranceMatches = evaluatedRows.filter((item) => item.insuranceMatch).length;
      const zipMatches = evaluatedRows.filter((item) => item.zipMatch).length;
      const nearMatches = evaluatedRows
        .filter((item) => item.score > 0)
        .sort((a, b) => b.score - a.score)
        .slice(0, 3)
        .map((item) => ({
          physicianName: getPhysicianName(item.row, config.columns.physicianName) || "Unknown",
          specialty: previewValue(item.aoeValue),
          insurance: previewValue(item.insuranceValue),
          zip: previewValue(item.zipValues),
          matchedCriteria: [
            item.specialtyMatch ? "specialty" : null,
            item.insuranceMatch ? "insurance" : null,
            item.zipMatch ? "zip" : null
          ].filter(Boolean)
        }));
      const zipMatchedExamples = evaluatedRows
        .filter((item) => item.zipMatch)
        .slice(0, 5)
        .map((item) => ({
          physicianName: getPhysicianName(item.row, config.columns.physicianName) || "Unknown",
          specialty: previewValue(item.aoeValue),
          insurance: previewValue(item.insuranceValue),
          zip: previewValue(item.zipValues)
        }));
      const sampleRowKeys = Object.keys(
        hubdbRows.find((row) => row?.values && typeof row.values === "object")?.values || {}
      );

      console.log(
        JSON.stringify({
          event: "referral_lookup_no_match",
          criteria: { specialtyNeeded, insurance, zip },
          columns: config.columns,
          counts: {
            totalRows: evaluatedRows.length,
            specialtyMatches,
            insuranceMatches,
            zipMatches
          },
          nearMatches,
          zipMatchedExamples,
          sampleRowKeys
        })
      );

      return {
        status: "success",
        contactName,
        createdCount: 0,
        results: [],
        message:
          "No physicians match this patient’s criteria. " +
          `Matched specialty in ${specialtyMatches} row(s), insurance in ${insuranceMatches}, and ZIP in ${zipMatches}.`,
        diagnostics: {
          totalRows: evaluatedRows.length,
          specialtyMatches,
          insuranceMatches,
          zipMatches,
          nearMatches,
          zipMatchedExamples
        }
      };
    }

    const randomized = shuffle(filtered).slice(0, 3);

    const created = [];
    for (const row of randomized) {
      const physicianName = getPhysicianName(row, config.columns.physicianName);
      const npi = clean(readColumn(row, config.columns.npi));
      const aoe = firstNonEmptyValue([
        readColumn(row, config.columns.aoe),
        readColumn(row, "area_of_expertise_aoe"),
        readColumn(row, "specialty")
      ]);
      const providerZip = firstNonEmptyValue([
        ...config.columns.zips.map((key) => readColumn(row, key)),
        readColumn(row, "office_1_address_zip"),
        readColumn(row, "office_2_address_zip"),
        readColumn(row, "office_3_address_zip"),
        readColumn(row, "office_4_address_zip"),
        readColumn(row, "office_5_address_zip")
      ]);
      const phone = firstNonEmptyValue([
        readColumn(row, config.columns.phone),
        readColumn(row, "office_1_address_telephone")
      ]);
      const fax = clean(readColumn(row, config.columns.fax));
      const address = firstNonEmptyValue([
        readColumn(row, config.columns.address),
        readColumn(row, "office_1_address")
      ]);
      const hubdbRowId = String(row.id || row.hs_id || "");

      const referralPayload = {
        properties: {
          [config.referralProperties.physicianName]: physicianName,
          [config.referralProperties.physicianNpi]: npi,
          [config.referralProperties.specialty]: aoe,
          [config.referralProperties.location]: address || providerZip,
          [config.referralProperties.phone]: phone,
          [config.referralProperties.referredBy]: "Physician Referral Lookup Card",
          [config.referralProperties.searchInsurance]: insurance,
          [config.referralProperties.searchLocation]: zip,
          [config.referralProperties.searchSpecialty]: specialtyNeeded,
          [config.referralProperties.hubdbRowId]: hubdbRowId,
          [config.referralProperties.referralDate]:
            new Date().toISOString().split("T")[0]
        }
      };

      if (pmi) {
        referralPayload.properties[config.referralProperties.patientMedicalId] = pmi;
      }

      const referralRecord = await createCustomObjectRecord(
        accessToken,
        config.referralObjectType,
        referralPayload
      );

      await associateReferralToContact(
        accessToken,
        config.referralObjectType,
        referralRecord.id,
        contactId,
        config.contactToReferralAssociationTypeId,
        config.contactToReferralAssociationCategory
      );

      created.push({
        id: referralRecord.id,
        hubdbRowId,
        physicianName,
        phone,
        fax,
        address
      });
    }

    return {
      status: "success",
      contactName,
      createdCount: created.length,
      results: created
    };
  } catch (e) {
    return error(e.message || "Unexpected server error.");
  }
};

function clean(value) {
  return stringifyValue(value).trim();
}

function matchesText(source, target) {
  if (!source || !target) return false;
  const targetNormalized = normalizeText(target);
  return toValueList(source).some((candidate) => {
    const normalized = normalizeText(candidate);
    return (
      normalized === targetNormalized ||
      normalized.includes(targetNormalized) ||
      targetNormalized.includes(normalized)
    );
  });
}

function matchesInsurance(source, target) {
  if (!source || !target) return false;
  const targetNormalized = normalizeText(target);
  return toValueList(source).some((candidate) => {
    const normalized = normalizeText(candidate);
    return (
      normalized === targetNormalized ||
      normalized.includes(targetNormalized) ||
      targetNormalized.includes(normalized)
    );
  });
}

function matchesZip(source, target) {
  if (!source || !target) return false;
  const targetZip = normalizeZip(target);
  if (!targetZip) return false;

  return toValueList(source).some((candidate) => normalizeZip(candidate) === targetZip);
}

function matchesAnyZip(sources, target) {
  return sources.some((source) => matchesZip(source, target));
}

function readColumn(row, key) {
  if (!row) return "";
  if (row.values && key in row.values) return row.values[key];
  if (key in row) return row[key];
  return "";
}

function shuffle(items) {
  const arr = [...items];
  for (let i = arr.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

function error(message) {
  return {
    status: "error",
    message
  };
}

async function hubspotFetch(path, token, options = {}) {
  const response = await fetch(`${HUBSPOT_BASE_URL}${path}`, {
    ...options,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(options.headers || {})
    }
  });

  const data = await response.json().catch(() => ({}));

  if (!response.ok) {
    const message =
      data?.message ||
      data?.error ||
      `HubSpot API error (${response.status})`;
    throw new Error(message);
  }

  return data;
}

async function getContact(token, contactId, properties = []) {
  const query = new URLSearchParams();
  properties.forEach((p) => query.append("properties", p));

  return hubspotFetch(
    `/crm/v3/objects/contacts/${contactId}?${query.toString()}`,
    token,
    { method: "GET" }
  );
}

async function getHubDbRows(token, tableId) {
  const rows = [];
  let after;

  do {
    const query = new URLSearchParams();
    if (after) query.set("after", after);

    const result = await hubspotFetch(
      `/cms/v3/hubdb/tables/${tableId}/rows${query.toString() ? `?${query.toString()}` : ""}`,
      token,
      { method: "GET" }
    );

    rows.push(...(result?.results || []));
    after = result?.paging?.next?.after;
  } while (after);

  return rows;
}

async function createCustomObjectRecord(token, objectType, payload) {
  return hubspotFetch(
    `/crm/v3/objects/${objectType}`,
    token,
    {
      method: "POST",
      body: JSON.stringify(payload)
    }
  );
}

async function associateReferralToContact(
  token,
  referralObjectType,
  referralId,
  contactId,
  associationTypeId,
  associationCategory = "USER_DEFINED"
) {
  if (!associationTypeId) {
    return hubspotFetch(
      `/crm/v4/objects/${referralObjectType}/${referralId}/associations/default/contact/${contactId}`,
      token,
      {
        method: "PUT"
      }
    );
  }

  return hubspotFetch(
    `/crm/v4/objects/${referralObjectType}/${referralId}/associations/contact/${contactId}`,
    token,
    {
      method: "PUT",
      body: JSON.stringify([
        {
          associationCategory,
          associationTypeId: Number(associationTypeId)
        }
      ])
    }
  );
}

async function getContactProperty(token, contactId, propertyName) {
  const contact = await getContact(token, contactId, [propertyName]);
  return clean(contact?.properties?.[propertyName]);
}

function stringifyValue(value) {
  if (value == null) return "";
  if (Array.isArray(value)) return value.map(stringifyValue).filter(Boolean).join("; ");
  if (typeof value === "object") {
    if (typeof value.label === "string") return value.label;
    if (typeof value.name === "string") return value.name;
    if (typeof value.value === "string") return value.value;
    return "";
  }
  return String(value);
}

function toValueList(value) {
  return stringifyValue(value)
    .split(/[;,]/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function normalizeText(value) {
  return stringifyValue(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

function normalizeZip(value) {
  const digits = stringifyValue(value).replace(/\D/g, "");
  return digits.slice(0, 5);
}

function previewValue(value) {
  const list = toValueList(value);
  return list.slice(0, 3).join("; ");
}

function parseColumnList(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function firstNonEmptyValue(values) {
  for (const value of values) {
    const cleaned = clean(value);
    if (cleaned) return cleaned;
  }
  return "";
}

function uniqueValues(values) {
  const seen = new Set();
  const result = [];

  for (const value of values) {
    const cleaned = clean(value);
    if (!cleaned || seen.has(cleaned)) continue;
    seen.add(cleaned);
    result.push(cleaned);
  }

  return result;
}

function getPhysicianName(row, configuredColumn) {
  const configuredName = configuredColumn ? clean(readColumn(row, configuredColumn)) : "";
  if (configuredName) return configuredName;

  const hsName = clean(readColumn(row, "hs_name"));
  if (hsName) return hsName;

  const firstName = clean(readColumn(row, "first_name"));
  const lastName = clean(readColumn(row, "last_name"));
  return [firstName, lastName].filter(Boolean).join(" ").trim();
}
