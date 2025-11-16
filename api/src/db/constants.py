FIELD_MAPPINGS = {
    "dockets": {
        "fields": [
            "json_extract(f.content, '$.data.attributes.docketType') as docket_type",
            "CASE WHEN json_extract(f.content, '$.data.attributes.modifyDate') IS NOT NULL AND json_extract(f.content, '$.data.attributes.modifyDate') != 'null' THEN try_strptime(json_extract(f.content, '$.data.attributes.modifyDate'), '%Y-%m-%dT%H:%M:%SZ') ELSE NULL END as modify_date",
            "json_extract(f.content, '$.data.attributes.title') as title",
        ],
        "path_pattern": "docket/*.json",
    },
    "comments": {
        "fields": [
            "json_extract(f.content, '$.data.id') as comment_id",
            "json_extract(f.content, '$.data.attributes.category') as category",
            "json_extract(f.content, '$.data.attributes.comment') as comment",
            "json_extract(f.content, '$.data.attributes.documentType') as document_type",
            "CASE WHEN json_extract(f.content, '$.data.attributes.modifyDate') IS NOT NULL AND json_extract(f.content, '$.data.attributes.modifyDate') != 'null' THEN try_strptime(json_extract(f.content, '$.data.attributes.modifyDate'), '%Y-%m-%dT%H:%M:%SZ') ELSE NULL END as modify_date",
            "CASE WHEN json_extract(f.content, '$.data.attributes.postedDate') IS NOT NULL AND json_extract(f.content, '$.data.attributes.postedDate') != 'null' THEN try_strptime(json_extract(f.content, '$.data.attributes.postedDate'), '%Y-%m-%dT%H:%M:%SZ') ELSE NULL END as posted_date",
            "CASE WHEN json_extract(f.content, '$.data.attributes.receiveDate') IS NOT NULL AND json_extract(f.content, '$.data.attributes.receiveDate') != 'null' THEN try_strptime(json_extract(f.content, '$.data.attributes.receiveDate'), '%Y-%m-%dT%H:%M:%SZ') ELSE NULL END as receive_date",
            "json_extract(f.content, '$.data.attributes.subtype') as subtype",
            "json_extract(f.content, '$.data.attributes.title') as title",
            "CASE WHEN json_extract(f.content, '$.data.attributes.withdrawn') IS NOT NULL AND json_extract(f.content, '$.data.attributes.withdrawn') != 'null' THEN json_extract(f.content, '$.data.attributes.withdrawn')::BOOLEAN ELSE NULL END as withdrawn",
        ],
        "path_pattern": "comments/*.json",
    },
    "documents": {
        "fields": [
            "json_extract(f.content, '$.data.id') as document_id",
            "json_extract(f.content, '$.data.attributes.category') as category",
            "json_extract(f.content, '$.data.attributes.documentType') as document_type",
            "CASE WHEN json_extract(f.content, '$.data.attributes.commentStartDate') IS NOT NULL AND json_extract(f.content, '$.data.attributes.commentStartDate') != 'null' THEN try_strptime(json_extract(f.content, '$.data.attributes.commentStartDate'), '%Y-%m-%dT%H:%M:%SZ') ELSE NULL END as comment_start_date",
            "CASE WHEN json_extract(f.content, '$.data.attributes.commentEndDate') IS NOT NULL AND json_extract(f.content, '$.data.attributes.commentEndDate') != 'null' THEN try_strptime(json_extract(f.content, '$.data.attributes.commentEndDate'), '%Y-%m-%dT%H:%M:%SZ') ELSE NULL END as comment_end_date",
            "CASE WHEN json_extract(f.content, '$.data.attributes.modifyDate') IS NOT NULL AND json_extract(f.content, '$.data.attributes.modifyDate') != 'null' THEN try_strptime(json_extract(f.content, '$.data.attributes.modifyDate'), '%Y-%m-%dT%H:%M:%SZ') ELSE NULL END as modify_date",
            "CASE WHEN json_extract(f.content, '$.data.attributes.postedDate') IS NOT NULL AND json_extract(f.content, '$.data.attributes.postedDate') != 'null' THEN try_strptime(json_extract(f.content, '$.data.attributes.postedDate'), '%Y-%m-%dT%H:%M:%SZ') ELSE NULL END as posted_date",
            "CASE WHEN json_extract(f.content, '$.data.attributes.receiveDate') IS NOT NULL AND json_extract(f.content, '$.data.attributes.receiveDate') != 'null' THEN try_strptime(json_extract(f.content, '$.data.attributes.receiveDate'), '%Y-%m-%dT%H:%M:%SZ') ELSE NULL END as receive_date",
            "CASE WHEN json_extract(f.content, '$.data.attributes.pageCount') IS NOT NULL AND json_extract(f.content, '$.data.attributes.pageCount') != 'null' THEN json_extract(f.content, '$.data.attributes.pageCount')::INT ELSE NULL END as page_count",
            "CASE WHEN json_extract(f.content, '$.data.attributes.withdrawn') IS NOT NULL AND json_extract(f.content, '$.data.attributes.withdrawn') != 'null' THEN json_extract(f.content, '$.data.attributes.withdrawn')::BOOLEAN ELSE NULL END as withdrawn",
        ],
        "path_pattern": "documents/*.json",
    },
}
