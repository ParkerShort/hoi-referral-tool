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
      referralObjectType: process.env.REFERRAL_OBJECT_TYPE || "",
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
        areaOfExpertise:
          process.env.REFERRAL_PROP_AREA_OF_EXPERTISE || "area_of_expertise_aoe",
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
        referralDate: process.env.REFERRAL_PROP_REFERRAL_DATE || "referral_date",
        firstName: process.env.REFERRAL_PROP_FIRST_NAME || "first_name",
        lastName: process.env.REFERRAL_PROP_LAST_NAME || "last_name",
        middleName: process.env.REFERRAL_PROP_MIDDLE_NAME || "middle_name",
        salutation: process.env.REFERRAL_PROP_SALUTATION || "salutation",
        gender: process.env.REFERRAL_PROP_GENDER || "gender",
        title: process.env.REFERRAL_PROP_TITLE || "title",
        medicalGroup: process.env.REFERRAL_PROP_MEDICAL_GROUP || "medical_group",
        specialtyCode:
          process.env.REFERRAL_PROP_SPECIALTY_CODE || "specialty_code",
        acceptingNewPatients:
          process.env.REFERRAL_PROP_ACCEPTING_NEW_PATIENTS ||
          "accepting_new_patients",
        physicianAcceptsReferrals:
          process.env.REFERRAL_PROP_PHYSICIAN_ACCEPTS_REFERRALS ||
          "physician_accepts_referrals",
        boardCertifications:
          process.env.REFERRAL_PROP_BOARD_CERTIFICATIONS ||
          "board_certifications",
        keywordsBySpecialty:
          process.env.REFERRAL_PROP_KEYWORDS_BY_SPECIALTY ||
          "keywords_by_specialty",
        stateLicenseNo:
          process.env.REFERRAL_PROP_STATE_LICENSE_NO || "state_license_no",
        stateLicenseState:
          process.env.REFERRAL_PROP_STATE_LICENSE_STATE ||
          "state_license_state",
        dea: process.env.REFERRAL_PROP_DEA || "dea",
        insurancesAccepted:
          process.env.REFERRAL_PROP_INSURANCES_ACCEPTED ||
          "insurances_accepted",
        status: process.env.REFERRAL_PROP_STATUS || "status"
      }
    };

    if (!config.hubdbTableId) {
      return error("Missing HUBDB_TABLE_ID secret.");
    }
    if (!config.referralObjectType) {
      return error(
        "Missing REFERRAL_OBJECT_TYPE secret. Set this to the live HubSpot custom object type ID for referrals."
      );
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
    const discoveredRowKeys = discoverHubDbKeys(hubdbRows);
    const schemaIssues = validateHubDbSchema(discoveredRowKeys, config.columns);

    if (schemaIssues.length) {
      console.log(
        JSON.stringify({
          event: "referral_lookup_schema_mismatch",
          columns: config.columns,
          discoveredRowKeys,
          schemaIssues
        })
      );

      return error(
        "Live HubDB table schema does not match this function's expected columns. " +
          schemaIssues.join(" ")
      );
    }

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
      const zipMatchedInsuranceSamples = uniqueValues(
        zipMatchedExamples.map((item) => item.insurance)
      ).slice(0, 3);
      const zipMatchedSpecialtySamples = uniqueValues(
        zipMatchedExamples.map((item) => item.specialty)
      ).slice(0, 3);
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
          zipMatchedInsuranceSamples,
          zipMatchedSpecialtySamples,
          discoveredRowKeys
        })
      );

      const zipMatchedInsuranceNote = zipMatchedInsuranceSamples.length
        ? ` ZIP-matched insurance samples: ${zipMatchedInsuranceSamples.join(" | ")}.`
        : "";
      const zipMatchedSpecialtyNote = zipMatchedSpecialtySamples.length
        ? ` ZIP-matched specialty samples: ${zipMatchedSpecialtySamples.join(" | ")}.`
        : "";

      return {
        status: "success",
        contactName,
        createdCount: 0,
        results: [],
        message:
          "No physicians match this patient’s criteria. " +
          `Matched specialty in ${specialtyMatches} row(s), insurance in ${insuranceMatches}, and ZIP in ${zipMatches}.` +
          zipMatchedSpecialtyNote +
          zipMatchedInsuranceNote,
        diagnostics: {
          totalRows: evaluatedRows.length,
          specialtyMatches,
          insuranceMatches,
          zipMatches,
          nearMatches,
          zipMatchedExamples,
          zipMatchedInsuranceSamples,
          zipMatchedSpecialtySamples
        }
      };
    }

    const randomized = shuffle(filtered).slice(0, 3);

    const created = [];
    for (const row of randomized) {
      const physicianName = getPhysicianName(row, config.columns.physicianName);
      const firstName = clean(readColumn(row, "first_name"));
      const lastName = clean(readColumn(row, "last_name"));
      const middleName = clean(readColumn(row, "middle_name"));
      const salutation = clean(readColumn(row, "salutation"));
      const gender = clean(readColumn(row, "gender"));
      const title = clean(readColumn(row, "title"));
      const medicalGroup = clean(readColumn(row, "medical_group"));
      const npi = clean(readColumn(row, config.columns.npi));
      const aoe = firstNonEmptyValue([
        readColumn(row, config.columns.aoe),
        readColumn(row, "area_of_expertise_aoe"),
        readColumn(row, "specialty")
      ]);
      const specialtyCode = clean(readColumn(row, "specialtycode"));
      const insurancesAccepted = clean(
        firstNonEmptyValue([
          readColumn(row, config.columns.insurance),
          readColumn(row, "insurances_accepted")
        ])
      );
      const acceptingNewPatients = clean(readColumn(row, "accepting_new_patients"));
      const physicianAcceptsReferrals = clean(
        readColumn(row, "physician_accepts_referrals")
      );
      const boardCertifications = clean(readColumn(row, "board_certifications"));
      const keywordsBySpecialty = clean(readColumn(row, "keywords_by_specialty"));
      const stateLicenseNo = clean(readColumn(row, "state_license_no"));
      const stateLicenseState = clean(readColumn(row, "state_license_state"));
      const dea = clean(readColumn(row, "dea"));
      const status = clean(readColumn(row, "status"));
      const offices = getOffices(row);
      const matchedOffice = findBestMatchingOffice(offices, zip);
      const primaryOffice = matchedOffice || offices.find(hasOfficeData) || null;
      const providerZip = primaryOffice?.zip || "";
      const phone = firstNonEmptyValue([
        primaryOffice?.telephone,
        readColumn(row, config.columns.phone),
        readColumn(row, "office_1_address_telephone")
      ]);
      const fax = clean(readColumn(row, config.columns.fax));
      const address = firstNonEmptyValue([
        primaryOffice?.address,
        readColumn(row, config.columns.address),
        readColumn(row, "office_1_address")
      ]);
      const hubdbRowId = String(row.id || row.hs_id || "");
      const normalizedReferralSpecialty = normalizeReferralSpecialty(aoe);

      const referralPayload = {
        properties: {}
      };

      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.physicianName,
        physicianName
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.firstName,
        firstName
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.lastName,
        lastName
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.middleName,
        middleName
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.salutation,
        salutation
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.gender,
        gender
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.title,
        title
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.medicalGroup,
        medicalGroup
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.physicianNpi,
        npi
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.areaOfExpertise,
        aoe
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.specialty,
        normalizedReferralSpecialty
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.specialtyCode,
        specialtyCode
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.location,
        address || providerZip
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.phone,
        phone
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.insurancesAccepted,
        insurancesAccepted
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.acceptingNewPatients,
        acceptingNewPatients
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.physicianAcceptsReferrals,
        physicianAcceptsReferrals
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.boardCertifications,
        boardCertifications
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.keywordsBySpecialty,
        keywordsBySpecialty
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.stateLicenseNo,
        stateLicenseNo
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.stateLicenseState,
        stateLicenseState
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.dea,
        dea
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.status,
        status
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.referredBy,
        "Physician Referral Lookup Card"
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.searchInsurance,
        insurance
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.searchLocation,
        zip
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.searchSpecialty,
        specialtyNeeded
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.hubdbRowId,
        hubdbRowId
      );
      addPropertyIfPresent(
        referralPayload.properties,
        config.referralProperties.referralDate,
        new Date().toISOString().split("T")[0]
      );

      for (const office of offices) {
        addPropertyIfPresent(
          referralPayload.properties,
          `office_${office.index}_address`,
          office.address
        );
        addPropertyIfPresent(
          referralPayload.properties,
          `office_${office.index}_address_city`,
          office.city
        );
        addPropertyIfPresent(
          referralPayload.properties,
          `office_${office.index}_address_state`,
          office.state
        );
        addPropertyIfPresent(
          referralPayload.properties,
          `office_${office.index}_address_zip`,
          office.zip
        );
        addPropertyIfPresent(
          referralPayload.properties,
          `office_${office.index}_address_telephone`,
          office.telephone
        );
      }

      if (pmi) {
        addPropertyIfPresent(
          referralPayload.properties,
          config.referralProperties.patientMedicalId,
          pmi
        );
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
    if (
      typeof data?.message === "string" &&
      data.message.includes("Unable to infer object type from")
    ) {
      throw new Error(
        `${data.message} Set REFERRAL_OBJECT_TYPE to the live custom object type ID (for example, 2-12345678) instead of a placeholder name.`
      );
    }
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
    if (typeof value.value === "string") return value.value;
    if (typeof value.name === "string") return value.name;
    if (typeof value.label === "string") return value.label;
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

function addPropertyIfPresent(target, key, value) {
  const cleanedKey = clean(key);
  if (!cleanedKey) return;

  if (value == null) return;
  const normalizedValue =
    typeof value === "string" ? value.trim() : stringifyValue(value).trim();

  if (!normalizedValue) return;
  target[cleanedKey] = normalizedValue;
}

function getOffices(row) {
  const offices = [];

  for (let index = 1; index <= 5; index += 1) {
    offices.push({
      index,
      address: clean(readColumn(row, `office_${index}_address`)),
      city: clean(readColumn(row, `office_${index}_address_city`)),
      state: clean(readColumn(row, `office_${index}_address_state`)),
      zip: clean(readColumn(row, `office_${index}_address_zip`)),
      telephone: clean(readColumn(row, `office_${index}_address_telephone`))
    });
  }

  return offices;
}

function hasOfficeData(office) {
  return Boolean(
    office &&
      (office.address || office.city || office.state || office.zip || office.telephone)
  );
}

function findBestMatchingOffice(offices, targetZip) {
  const normalizedTargetZip = normalizeZip(targetZip);
  if (!normalizedTargetZip) return null;

  return (
    offices.find((office) => normalizeZip(office?.zip) === normalizedTargetZip) ||
    null
  );
}

function normalizeReferralSpecialty(value) {
  const normalized = clean(value);
  if (!normalized) return "";

  const specialtyMap = {
    orthopedic_surgery: "orthopedic_surgery",
    sports_medicine: "sports_medicine",
    pain_medicine: "pain_medicine",
    neurosurgery: "neurosurgery",
    physical_medicine_and_rehabilitation:
      "physical_medicine_and_rehabilitation",
    "non-operative_sports_medicine": "sports_medicine",
    non_operative_sports_medicine: "sports_medicine",
    shoulder_surgery: "orthopedic_surgery",
    knee_surgery: "orthopedic_surgery",
    hip_surgery: "orthopedic_surgery",
    spine_surgery: "orthopedic_surgery",
    joint_replacement_surgery: "orthopedic_surgery",
    hand_and_wrist_surgery: "orthopedic_surgery",
    foot_and_ankle_surgery: "orthopedic_surgery",
    elbow_surgery: "orthopedic_surgery",
    trauma_surgery: "orthopedic_surgery"
  };

  return specialtyMap[normalized] || "";
}

function discoverHubDbKeys(rows) {
  const keySet = new Set();

  for (const row of rows || []) {
    if (row && typeof row === "object") {
      Object.keys(row).forEach((key) => keySet.add(key));
    }

    if (row?.values && typeof row.values === "object") {
      Object.keys(row.values).forEach((key) => keySet.add(key));
    }
  }

  return [...keySet];
}

function validateHubDbSchema(discoveredRowKeys, columns) {
  const keySet = new Set(discoveredRowKeys || []);
  const issues = [];

  if (!discoveredRowKeys?.length) {
    return ["No HubDB row keys were returned from the live table."];
  }

  if (!keySet.has(columns.aoe) && !keySet.has("specialty")) {
    issues.push(
      `Expected specialty column '${columns.aoe}' was not found in live table keys.`
    );
  }

  if (!keySet.has(columns.insurance)) {
    issues.push(
      `Expected insurance column '${columns.insurance}' was not found in live table keys.`
    );
  }

  const hasAnyZipColumn = (columns.zips || []).some((key) => keySet.has(key));
  if (!hasAnyZipColumn) {
    issues.push(
      `None of the expected ZIP columns were found: ${(columns.zips || []).join(", ")}.`
    );
  }

  return issues;
}
