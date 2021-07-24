import elasticsearch
import plac
import tqdm
import numpy as np
import torch
import torch.nn.functional as F
import asyncio 
import sentence_transformers
import spacy
from typing import List, Union
import jsonlines

INDEX_NAME = "clinical_test"
DOCUMENT_FP = "es_index.json"
MODEL_FP = "model/"

es = elasticsearch.Elasticsearch()
sentence_encoder = sentence_transformers.SentenceTransformer(MODEL_FP)
nlp = spacy.load("en_core_sci_sm", disable=['ner', 'tagger'])
nlp.max_length = 2000000

best_fields = ["LocationCountries.Country", "BiospecRetention", "DetailedDescription.Textblock", "HasExpandedAccess",
                            "ConditionBrowse.MeshTerm", "RequiredHeader.LinkText", "WhyStopped", "BriefSummary.Textblock",
                            "Eligibility.Criteria.Textblock", "OfficialTitle", "Eligibility.MaximumAge", "Eligibility.StudyPop.Textblock",
                            "BiospecDescr.Textblock", "BriefTitle", "Eligibility.MinimumAge", "ResponsibleParty.Organization",
                            "TargetDuration", "Condition", "IDInfo.OrgStudyID", "Keyword", "Source", "Sponsors.LeadSponsor.Agency",
                            "ResponsibleParty.InvestigatorAffiliation", "OversightInfo.Authority", "OversightInfo.HasDmc", "OverallContact.Phone",
                            "Phase", "OverallContactBackup.LastName", "Acronym", "InterventionBrowse.MeshTerm", "RemovedCountries.Country"]


async def encode_field(field: Union[str,List[str]]=None):
    await asyncio.sleep(0.1)

    embedding = sentence_encoder.encode(field, convert_to_tensors=True),

    if len(embedding.size()) == 1:
        embeddings = torch.unsqueeze(embedding)

    if field:
        return F.norm(torch.mean(embeddings, axis=0), dim=0)

    return [0]*768


def retrieve_field(document, field):
    fields = field.split(".")

    for field in fields:
        document = document[field]

    return document


def add_field(document, field, value):
    field = field.replace(".", '_')+'_Embedding'

    document[field] = value


async def encode_document(document):
    await asyncio.sleep(0.1)

    update_document = {}

    for field in best_fields:
        embed_field = retrieve_field(document, field)

        if field.endswith('Textblock'):
            embed_field = [' '.join(sent.text.split()) for sent in nlp(doc['abstract']).sents if sent.text.strip()]

        embedding = encode_field(embed_field)
        add_field(update_document, field, embedding)

    return update_document


async def index_documents(parsed_ids, document_itr, index_name):
    with open('parsed_ids.txt', 'a+') as parsed_writer:
        for _, document in tqdm.tqdm(document_itr):
            _id = document['_id']

            document = document['_source']

            if _id in parsed_ids:
                continue

            try:
                update_document = await encode_document(document)
                es.update(index=index_name, id=_id, body=update_document)
                parsed_writer.write(f"{_id}\n")
            except:
                print(f"Cannot process doc {_id}")
                pass


if __name__ == "__main__":
    document_reader = jsonlines.open(DOCUMENT_FP)
    parsed_ids = open("parsed_ids.txt", "r+").readlines()

    asyncio.run(index_documents(parsed_ids, document_itr=document_reader, index_name=INDEX_NAME))
