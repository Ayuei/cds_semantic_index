import elasticsearch
import plac
import tqdm
import numpy as np
import torch
import asyncio 
import os
import spacy

best_fields = ["LocationCountries.Country", "BiospecRetention", "DetailedDescription.Textblock", "HasExpandedAccess",
                            "ConditionBrowse.MeshTerm", "RequiredHeader.LinkText", "WhyStopped", "BriefSummary.Textblock",
                            "Eligibility.Criteria.Textblock", "OfficialTitle", "Eligibility.MaximumAge", "Eligibility.StudyPop.Textblock",
                            "BiospecDescr.Textblock", "BriefTitle", "Eligibility.MinimumAge", "ResponsibleParty.Organization",
                            "TargetDuration", "Condition", "IDInfo.OrgStudyID", "Keyword", "Source", "Sponsors.LeadSponsor.Agency",
                            "ResponsibleParty.InvestigatorAffiliation", "OversightInfo.Authority", "OversightInfo.HasDmc", "OverallContact.Phone",
                            "Phase", "OverallContactBackup.LastName", "Acronym", "InterventionBrowse.MeshTerm", "RemovedCountries.Country"]


async def embed_field(bc, field: List[str]=None):
    await asyncio.sleep(0.1)

    if field:
        return torch.mean(bc.encode(field, numpy=False), axis=0)


async def index_documents(parsed_ids, json_lines, index_name):
    nlp = spacy.load("en_core_sci_sm", disable=['ner', 'tagger'])
    nlp.max_length = 2000000
