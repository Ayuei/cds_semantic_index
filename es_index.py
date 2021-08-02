<<<<<<< HEAD
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
=======
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


def reverse_readline(filename, buf_size=8192):
    """A generator that returns the lines of a file in reverse order"""
    with open(filename) as fh:
        segment = None
        offset = 0
        fh.seek(0, os.SEEK_END)
        file_size = remaining_size = fh.tell()
        while remaining_size > 0:
            offset = min(file_size, offset + buf_size)
            fh.seek(file_size - offset)
            buffer = fh.read(min(remaining_size, buf_size))
            remaining_size -= buf_size
            lines = buffer.split('\n')
            # The first line of the buffer is probably not a complete line so
            # we'll save it and append it to the last line of the next buffer
            # we read
            if segment is not None:
                # If the previous chunk starts right from the beginning of line
                # do not concat the segment to the last line of new chunk.
                # Instead, yield the segment first
                if buffer[-1] != '\n':
                    lines[-1] += segment
                else:
                    yield segment
            segment = lines[0]
            for index in range(len(lines) - 1, 0, -1):
                if lines[index]:
                    yield lines[index]
        # Don't yield None if the file was empty
        if segment is not None:
            yield segment


INDEX_NAME = "test_trials"
MODEL_FP = "model/"
NUBMER = sys.argv[1]
DOCUMENT_FP = "es_index.jsona"+NUMBER

es = elasticsearch.AsyncElasticsearch([{'host': ""}])
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


async def encode_field(field: Union[str, List[str]] = None):
    await asyncio.sleep(0.1)

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


async def encode_document(document):
    await asyncio.sleep(0.1)

    update_document = {}

    for field in best_fields:
        embed_field = retrieve_field(document, field)

        if field.endswith('Textblock') and embed_field:
            embed_field = [' '.join(sent.text.split()) for sent in nlp(embed_field).sents if sent.text.strip()]

        embedding = await encode_field(embed_field)

        if embedding:
            add_field(update_document, field, embedding)

    return update_document


async def index_documents(parsed_ids, document_itr, index_name):
    await update_mappings()
    with open('parsed_ids.txt'+NUMBER, 'a+') as parsed_writer:
        for document in tqdm.tqdm(document_itr, total=2.03e5):
            document = json.loads(document)
            _id = document['_id']

            try:
                document = document['_source']

                if _id in parsed_ids:
                    print(f'Skipping {_id}')
                    continue

                update_document = await encode_document(document)
                await es.update(index=index_name, id=_id, body={'doc': update_document})
                parsed_writer.write(f"{_id}\n")
            except Exception as e:
                print(f"Cannot parse {_id}: {e}")


if __name__ == "__main__":
    document_reader = reverse_readline(DOCUMENT_FP)
    parsed_ids = open("parsed_ids.txt"+NUMBER, "r+").readlines()

    asyncio.run(index_documents(parsed_ids, document_itr=document_reader, index_name=INDEX_NAME))
>>>>>>> d6e9ed6a5c3d79640cef3c91dbbaeebfcc423bc0
