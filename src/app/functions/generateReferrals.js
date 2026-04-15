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
        aoe: process.env.HUBDB_COL_AOE || "area_of_expertise",
        insurance: process.env.HUBDB_COL_INSURANCE || "insurances_accepted",
        zip: process.env.HUBDB_COL_ZIP || "office_1_address_zip",
        physicianName: process.env.HUBDB_COL_PHYSICIAN_NAME || "physician_name",
        npi: process.env.HUBDB_COL_NPI || "npi",
        phone: process.env.HUBDB_COL_PHONE || "phone",
        fax: process.env.HUBDB_COL_FAX || "fax",
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

    const specialtyNeeded = clean(sentProps.specialty_needed_temporary);
    const insurance = clean(
      sentProps.what_insurance_plan_and_company_are_you_using
    );
    const zip = clean(sentProps.zip);

    const pmi = config.pmiProperty
      ? await getContactProperty(accessToken, contactId, config.pmiProperty)
      : "";

    const missing = [];
    if (!specialtyNeeded) missing.push("specialty_needed_temporary");
    if (!insurance) missing.push("what_insurance_plan_and_company_are_you_using");
    if (!zip) missing.push("zip");

    if (missing.length) {
      return {
        status: "error",
        message: `Missing required contact properties: ${missing.join(", ")}`
      };
    }

    const hubdbRows = await getHubDbRows(accessToken, config.hubdbTableId);

    const filtered = hubdbRows.filter((row) => {
      const aoeValue = clean(readColumn(row, config.columns.aoe));
      const insuranceValue = clean(readColumn(row, config.columns.insurance));
      const zipValue = clean(readColumn(row, config.columns.zip));

      return (
        matchesText(aoeValue, specialtyNeeded) &&
        matchesInsurance(insuranceValue, insurance) &&
        matchesZip(zipValue, zip)
      );
    });

    if (!filtered.length) {
      return {
        status: "success",
        contactName,
        createdCount: 0,
        results: [],
        message: "No physicians match this patient’s criteria."
      };
    }

    const randomized = shuffle(filtered).slice(0, 3);

    const created = [];
    for (const row of randomized) {
      const physicianName = clean(readColumn(row, config.columns.physicianName));
      const npi = clean(readColumn(row, config.columns.npi));
      const aoe = clean(readColumn(row, config.columns.aoe));
      const providerZip = clean(readColumn(row, config.columns.zip));
      const phone = clean(readColumn(row, config.columns.phone));
      const fax = clean(readColumn(row, config.columns.fax));
      const address = clean(readColumn(row, config.columns.address));
      const hubdbRowId = String(row.id || "");

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
  return String(value || "").trim();
}

function matchesText(source, target) {
  if (!source || !target) return false;
  return source.toLowerCase() === target.toLowerCase();
}

function matchesInsurance(source, target) {
  if (!source || !target) return false;

  const sourceLower = source.toLowerCase();
  const targetLower = target.toLowerCase();

  return (
    sourceLower === targetLower ||
    sourceLower.includes(targetLower) ||
    targetLower.includes(sourceLower)
  );
}

function matchesZip(source, target) {
  if (!source || !target) return false;
  return String(source).trim() === String(target).trim();
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
  const result = await hubspotFetch(
    `/cms/v3/hubdb/tables/${tableId}/rows`,
    token,
    { method: "GET" }
  );

  return result?.results || [];
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
