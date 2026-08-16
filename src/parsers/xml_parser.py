# src/parsers/xml_parser.py

import xml.etree.ElementTree as ET

from src.common.exceptions import (
    XMLParsingError,
)

from src.common.logging_config import (
    get_logger,
)


logger = get_logger(__name__)


# ============================================================
# COMMON HELPER
# ============================================================

def _text(
    element,
    path: str,
):
    """
    Safely return XML element text.
    """

    if element is None:
        return None

    child = element.find(
        path
    )

    if (
        child is None
        or child.text is None
    ):
        return None

    value = child.text.strip()

    return value or None


# ============================================================
# VISIT
# ============================================================

def _parse_visit(
    root,
) -> list[dict]:

    output = []


    visits = root.findall(
        "./Body/Visits/Visit"
    )


    for visit in visits:

        study = visit.find(
            "./References/Study"
        )

        subject = visit.find(
            "./References/Subject"
        )


        output.append(
            {

                "visit_id":
                    visit.get(
                        "id"
                    ),

                "study_id":
                    (
                        study.get("id")
                        if study is not None
                        else None
                    ),

                "subject_id":
                    (
                        subject.get("id")
                        if subject is not None
                        else None
                    ),

                "visit_name":
                    _text(
                        visit,
                        "./Schedule/VisitName",
                    ),

                "planned_date":
                    _text(
                        visit,
                        (
                            "./Schedule/"
                            "Dates/"
                            "PlannedDate"
                        ),
                    ),

                "actual_date":
                    _text(
                        visit,
                        (
                            "./Schedule/"
                            "Dates/"
                            "ActualDate"
                        ),
                    ),

                "updated_at":
                    _text(
                        visit,
                        "./Audit/UpdatedAt",
                    ),
            }
        )


    return output


# ============================================================
# ADVERSE EVENT
# ============================================================

def _parse_adverse_event(
    root,
) -> list[dict]:

    output = []


    events = root.findall(
        "./Body/AdverseEvents/AdverseEvent"
    )


    for event in events:

        study = event.find(
            "./References/Study"
        )

        subject = event.find(
            "./References/Subject"
        )

        seriousness = event.find(
            (
                "./ClinicalEvent/"
                "EventDetails/"
                "Classification/"
                "Seriousness"
            )
        )


        output.append(
            {

                "ae_id":
                    event.get(
                        "id"
                    ),

                "study_id":
                    (
                        study.get("id")
                        if study is not None
                        else None
                    ),

                "subject_id":
                    (
                        subject.get("id")
                        if subject is not None
                        else None
                    ),

                "event_term":
                    _text(
                        event,
                        (
                            "./ClinicalEvent/"
                            "EventDetails/"
                            "Term"
                        ),
                    ),

                "severity":
                    _text(
                        event,
                        (
                            "./ClinicalEvent/"
                            "EventDetails/"
                            "Classification/"
                            "Severity"
                        ),
                    ),

                "serious":
                    (
                        seriousness.get(
                            "flag"
                        )
                        if seriousness is not None
                        else None
                    ),

                "event_date":
                    _text(
                        event,
                        (
                            "./ClinicalEvent/"
                            "Timeline/"
                            "EventDate"
                        ),
                    ),

                "reported_date":
                    _text(
                        event,
                        (
                            "./ClinicalEvent/"
                            "Timeline/"
                            "ReportedDate"
                        ),
                    ),

                "processing_priority":
                    _text(
                        event,
                        (
                            "./SafetyAssessment/"
                            "ProcessingPriority"
                        ),
                    ),

                "requires_safety_review":
                    _text(
                        event,
                        (
                            "./SafetyAssessment/"
                            "RequiresSafetyReview"
                        ),
                    ),

                "updated_at":
                    _text(
                        event,
                        (
                            "./AuditTrail/"
                            "Timestamps/"
                            "LastUpdated"
                        ),
                    ),
            }
        )


    return output


# ============================================================
# DATA QUERY
# ============================================================

def _parse_data_query(
    root,
) -> list[dict]:
    """
    Flatten Data Query XML.

    ClinicalContext contains:

        Study
        Subject
        Site
    """

    output = []


    queries = root.findall(
        "./Body/Queries/DataQuery"
    )


    for query in queries:

        study = query.find(
            "./ClinicalContext/Study"
        )

        subject = query.find(
            "./ClinicalContext/Subject"
        )

        site = query.find(
            "./ClinicalContext/Site"
        )


        output.append(
            {

                "query_id":
                    query.get(
                        "id"
                    ),

                "study_id":
                    (
                        study.get("id")
                        if study is not None
                        else None
                    ),

                "subject_id":
                    (
                        subject.get("id")
                        if subject is not None
                        else None
                    ),

                "site_id":
                    (
                        site.get("id")
                        if site is not None
                        else None
                    ),

                "opened_date":
                    _text(
                        query,
                        (
                            "./Lifecycle/"
                            "OpenedDate"
                        ),
                    ),

                "resolved_date":
                    _text(
                        query,
                        (
                            "./Lifecycle/"
                            "ResolvedDate"
                        ),
                    ),

                "status":
                    _text(
                        query,
                        (
                            "./Lifecycle/"
                            "Status"
                        ),
                    ),

                "updated_at":
                    _text(
                        query,
                        (
                            "./AuditTrail/"
                            "UpdatedAt"
                        ),
                    ),
            }
        )


    return output


# ============================================================
# PUBLIC XML PARSER
# ============================================================

def parse_xml_response(
    entity_name: str,
    raw_text: str,
) -> list[dict]:

    logger.info(
        (
            "entity=%s "
            "xml_parsing_started"
        ),
        entity_name,
    )


    # ========================================================
    # EMPTY
    # ========================================================

    if not raw_text.strip():

        logger.info(
            (
                "entity=%s "
                "xml_response_empty"
            ),
            entity_name,
        )

        return []


    try:

        # ====================================================
        # PARSE XML
        # ====================================================

        root = ET.fromstring(
            raw_text
        )


        # ====================================================
        # ROUTE ENTITY
        # ====================================================

        if entity_name == "visit":

            records = _parse_visit(
                root
            )


        elif entity_name == "adverse_event":

            records = _parse_adverse_event(
                root
            )


        elif entity_name == "data_query":

            records = _parse_data_query(
                root
            )


        else:

            raise XMLParsingError(
                (
                    "No XML parser configured "
                    f"for entity={entity_name}"
                )
            )


        # ====================================================
        # SUCCESS
        # ====================================================

        logger.info(
            (
                "entity=%s "
                "xml_parsing_completed "
                "record_count=%s"
            ),
            entity_name,
            len(records),
        )


        return records


    except XMLParsingError:

        raise


    except ET.ParseError as exc:

        logger.exception(
            (
                "entity=%s "
                "invalid_xml"
            ),
            entity_name,
        )


        raise XMLParsingError(
            (
                "Invalid XML received "
                f"for entity={entity_name}"
            )
        ) from exc


    except Exception as exc:

        logger.exception(
            (
                "entity=%s "
                "xml_parsing_failed"
            ),
            entity_name,
        )


        raise XMLParsingError(
            (
                "Failed parsing XML "
                f"for entity={entity_name}"
            )
        ) from exc