import elasticsearch
import plac
import tqdm
import numpy as np
import torch
import asyncio 
import os
import spacy
from typing import List, Union

best_fields = ["LocationCountries.Country", "BiospecRetention", "DetailedDescription.Textblock", "HasExpandedAccess",
                            "ConditionBrowse.MeshTerm", "RequiredHeader.LinkText", "WhyStopped", "BriefSummary.Textblock",
                            "Eligibility.Criteria.Textblock", "OfficialTitle", "Eligibility.MaximumAge", "Eligibility.StudyPop.Textblock",
                            "BiospecDescr.Textblock", "BriefTitle", "Eligibility.MinimumAge", "ResponsibleParty.Organization",
                            "TargetDuration", "Condition", "IDInfo.OrgStudyID", "Keyword", "Source", "Sponsors.LeadSponsor.Agency",
                            "ResponsibleParty.InvestigatorAffiliation", "OversightInfo.Authority", "OversightInfo.HasDmc", "OverallContact.Phone",
                            "Phase", "OverallContactBackup.LastName", "Acronym", "InterventionBrowse.MeshTerm", "RemovedCountries.Country"]


async def embed_field(bc, field: Union[str,List[str]]=None):
    await asyncio.sleep(0.1)

    if field:
        return torch.mean(bc.encode(field, numpy=False), axis=0)

    return [0]*768
    

def retrieve_field(document, field):
    fields = field.split(".")

    for field in fields:
        document = document[field]

    return document


def add_field(document, field, value):
    fields = field.split(".")

    temp = {}
    orig = temp
    i = 0

    for field in fields:
        if i == len(field) - 1:
            temp[field] = value
            document.update(orig)
            break

        temp[field] = {}
        temp = temp[field]

        i+=1

    return document


async def encode_document(document):
    await asyncio.sleep(0.1)

    for field in best_fields:
        field = retrieve_field(document, field)
        embedding = embed_field(field)
        add_field(document, field, embedding)


async def index_documents(parsed_ids, json_lines, index_name):
    nlp = spacy.load("en_core_sci_sm", disable=['ner', 'tagger'])
    nlp.max_length = 2000000

    for _, document in tqdm(json_lines):
        _id = document['_id']
        try:
            encode_document(document)
            es.update(index=index_name, id=_id, body=document)
        except:
            print(f"Cannot process doc {_id}")
            pass
