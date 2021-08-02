import elasticsearch
import plac
import tqdm
import torch
import torch.nn.functional as F
import asyncio
import sentence_transformers
import spacy
from typing import List, Union
import jsonlines
import sys
import os
import json


INDEX_NAME = "test_trials"
MODEL_FP = "model/"
NUMBER= sys.argv[1]
DOCUMENT_FP = "es_index.json0"+NUMBER

es = elasticsearch.AsyncElasticsearch([{'host': "10.62.134.3"}])
sentence_encoder = sentence_transformers.SentenceTransformer(MODEL_FP)
nlp = spacy.load("en_core_sci_sm")
nlp.max_length = 2000000

best_fields = ["LocationCountries.Country", "BiospecRetention", "DetailedDescription.Textblock", "HasExpandedAccess",
               "ConditionBrowse.MeshTerm", "RequiredHeader.LinkText", "WhyStopped", "BriefSummary.Textblock",
               "Eligibility.Criteria.Textblock", "OfficialTitle", "Eligibility.MaximumAge",
               "Eligibility.StudyPop.Textblock",
               "BiospecDescr.Textblock", "BriefTitle", "Eligibility.MinimumAge", "ResponsibleParty.Organization",
               "TargetDuration", "Condition", "IDInfo.OrgStudyID", "Keyword", "Source", "Sponsors.LeadSponsor.Agency",
               "ResponsibleParty.InvestigatorAffiliation", "OversightInfo.Authority", "OversightInfo.HasDmc",
               "OverallContact.Phone",
               "Phase", "OverallContactBackup.LastName", "Acronym", "InterventionBrowse.MeshTerm",
               "RemovedCountries.Country"]


async def update_mappings():
    mapping = {}

    value = {"type": "dense_vector",
             "dims": 768}

    for field in best_fields:
        add_field(mapping, field, value)

    await es.indices.put_mapping(body={"properties": mapping}, index=INDEX_NAME)


def encode_field(field: Union[str, List[str]] = None):
    if field:
        embeddings = sentence_encoder.encode(field, convert_to_tensor=True)

        if len(embeddings.size()) == 1:
            embeddings = torch.unsqueeze(embeddings, dim=0)
        norm = F.normalize(torch.mean(embeddings, axis=0), dim=0)

        return norm.tolist()

    return False


def retrieve_field(document, field):
    fields = field.split(".")

    for field in fields:
        document = document[field]

    return document


def add_field(document, field, value):
    field = field.replace(".", '_') + '_Embedding'

    document[field] = value


def encode_document(document):
    update_document = {}

    for field in best_fields:
        embed_field = retrieve_field(document, field)

        if field.endswith('Textblock') and embed_field:
            embed_field = [' '.join(sent.text.split()) for sent in nlp(embed_field).sents if sent.text.strip()]

        embedding = encode_field(embed_field)

        if embedding:
            add_field(update_document, field, embedding)

    return update_document


async def index_documents(parsed_ids, document_itr, index_name):
    await update_mappings()
    with open('parsed_ids.txt'+NUMBER, 'a+') as parsed_writer:
        for document in tqdm.tqdm(document_itr, total=2.03e5//7):
            document = json.loads(document)
            _id = document['_id']

            try:
                document = document['_source']

                if _id+"\n" in parsed_ids:
                    print(f'Skipping {_id}')
                    continue

                update_document = encode_document(document)
                await es.update(index=index_name, id=_id, body={'doc': update_document})
                parsed_writer.write(f"{_id}\n")
            except Exception as e:
                print(f"Cannot parse {_id}: {e}")


if __name__ == "__main__":
    #document_reader = reverse_readline(DOCUMENT_FP)
    document_reader = open(DOCUMENT_FP, "r")
    parsed_ids = open("parsed_ids.txt"+NUMBER, "r+").readlines()

    asyncio.run(index_documents(parsed_ids, document_itr=document_reader, index_name=INDEX_NAME))
