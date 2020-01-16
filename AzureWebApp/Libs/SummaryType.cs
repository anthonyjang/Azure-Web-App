
namespace AzureWebApp.Libs
{
    public enum SummaryType
    {
        /// <summary>
        /// Return only those elements marked as "summary" in the base definition of the resource(s)
        /// </summary>
        True,

        /// <summary>
        /// Return only the "text" element, and any mandatory elements
        /// </summary>
        Text,

        /// <summary>
        /// Remove the text element
        /// </summary>
        Data,

        /// <summary>
        /// Search only: just return a count of the matching resources, without returning the actual matches
        /// </summary>
        Count,

        /// <summary>
        /// Return all parts of the resource(s)
        /// </summary>
        False
    }

    public enum RSSearchComparator
    {
        //
        // Summary:
        //     MISSING DESCRIPTION (system: http://hl7.org/fhir/search-comparator)
        Eq = 0,
        //
        // Summary:
        //     MISSING DESCRIPTION (system: http://hl7.org/fhir/search-comparator)
        Ne = 1,
        //
        // Summary:
        //     MISSING DESCRIPTION (system: http://hl7.org/fhir/search-comparator)
        Gt = 2,
        //
        // Summary:
        //     MISSING DESCRIPTION (system: http://hl7.org/fhir/search-comparator)
        Lt = 3,
        //
        // Summary:
        //     MISSING DESCRIPTION (system: http://hl7.org/fhir/search-comparator)
        Ge = 4,
        //
        // Summary:
        //     MISSING DESCRIPTION (system: http://hl7.org/fhir/search-comparator)
        Le = 5,
        //
        // Summary:
        //     MISSING DESCRIPTION (system: http://hl7.org/fhir/search-comparator)
        Sa = 6,
        //
        // Summary:
        //     MISSING DESCRIPTION (system: http://hl7.org/fhir/search-comparator)
        Eb = 7,
        //
        // Summary:
        //     MISSING DESCRIPTION (system: http://hl7.org/fhir/search-comparator)
        Ap = 8
    }


}
