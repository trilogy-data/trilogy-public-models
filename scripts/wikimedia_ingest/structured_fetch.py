#!/usr/bin/env python3
# /// script
# requires-python = ">=3.8"
# dependencies = [
#     "wikipedia",
#     "httpx",
#     "beautifulsoup4",
#     "google-generativeai",
# ]
# ///

import wikipedia
import httpx
from bs4 import BeautifulSoup
import google.generativeai as genai
import json
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field


@dataclass
class ExtractionField:
    name: str
    prompt: Optional[str] = None
    options: Optional[List[str]] = None
    multiple: bool = False


@dataclass
class WikipediaResult:
    title: str
    summary: str
    url: str
    image_url: Optional[str] = None


@dataclass
class ExtractionResult:
    fields: Dict[str, Any]


class WikipediaFetcher:
    def __init__(self, http_client: Optional[httpx.Client] = None):
        self.http_client = http_client or httpx.Client()

    def fetch(self, query: str, sentences: int = 3) -> WikipediaResult:
        search_results = wikipedia.search(query)
        if not search_results:
            raise ValueError(f"No Wikipedia results found for '{query}'")

        title = search_results[0]
        page = wikipedia.page(title, auto_suggest=False, redirect=True)
        summary = page.summary
        image_url = self._extract_image(page.url)

        return WikipediaResult(title, summary, page.url, image_url)

    def _extract_image(self, url: str) -> Optional[str]:
        response = self.http_client.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        infobox = soup.find("table", {"class": "infobox"})
        if infobox:
            img_tag = infobox.find("img")
            if img_tag and img_tag.has_attr("src"):
                src = img_tag["src"]
                return f"https:{src}" if isinstance(src, str) else None
        return None

    def validate_result(self, query: str, result: WikipediaResult) -> bool:
        if not result.summary:
            return False
        query_lower = query.lower()
        title_lower = result.title.lower()
        return query_lower in title_lower or title_lower in query_lower


class LLMExtractor:
    def __init__(self, model: Any):
        self.model = model

    def extract(
        self, fields: List[ExtractionField], context: str, subject: str
    ) -> ExtractionResult:
        prompt = self._build_prompt(fields, context, subject)
        response = self.model.generate_content(prompt)
        response_text = response.candidates[0].content.parts[0].text.strip()
        cleaned = self._clean_response(response_text)
        data = json.loads(cleaned)
        return ExtractionResult(fields=data)

    def _build_prompt(
        self, fields: List[ExtractionField], context: str, subject: str
    ) -> str:
        field_specs = []
        for f in fields:
            if f.prompt:
                spec = f.prompt
            elif f.options:
                spec = f"Select {'applicable options' if f.multiple else 'the most appropriate option'} from: {f.options}"
            else:
                spec = "Extract relevant information"
            field_specs.append(f'"{f.name}": {spec}')

        schema = {f.name: ([] if f.multiple else "") for f in fields}

        return f"""Analyze the following and extract structured information.

Subject: {subject}
Context: {context}

Extract the following fields:
{chr(10).join(field_specs)}

Return only a JSON object with this structure:
{json.dumps(schema, indent=2)}

Respond only with valid JSON, no additional text."""

    def _clean_response(self, response: str) -> str:
        if response.startswith("```json"):
            return response[7:-3].strip()
        elif response.startswith("```"):
            return response[3:-3].strip()
        return response


class WikipediaStructuredExtractor:
    def __init__(
        self,
        wiki_fetcher: WikipediaFetcher,
        llm_extractor: Optional[LLMExtractor] = None,
    ):
        self.wiki_fetcher = wiki_fetcher
        self.llm_extractor = llm_extractor

    def extract(
        self,
        query: str,
        fields: Optional[List[ExtractionField]] = None,
        validate: bool = True,
    ) -> Dict[str, Any]:
        wiki_result = self.wiki_fetcher.fetch(query)

        if validate and not self.wiki_fetcher.validate_result(query, wiki_result):
            raise ValueError(
                f"Wikipedia result for '{query}' returned '{wiki_result.title}' which appears unrelated"
            )

        result = {
            "query": query,
            "title": wiki_result.title,
            "summary": wiki_result.summary,
            "url": wiki_result.url,
            "image_url": wiki_result.image_url,
        }

        if fields and self.llm_extractor:
            extraction = self.llm_extractor.extract(
                fields, wiki_result.summary, query
            )
            result["extracted"] = extraction.fields

        return result


def create_extractor(
    api_key: Optional[str] = None, model_name: str = "gemini-2.5-flash"
) -> WikipediaStructuredExtractor:
    wiki_fetcher = WikipediaFetcher()

    llm_extractor = None
    if api_key:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(model_name)
        llm_extractor = LLMExtractor(model)
    else:
        raise ValueError("API key is required to use the LLM extractor")

    return WikipediaStructuredExtractor(wiki_fetcher, llm_extractor)


def main():
    import os

    extractor = create_extractor(os.getenv("GEMINI_API_KEY"))

    fields = [
        ExtractionField(
            "habitat",
            options=["Marine", "Freshwater", "Terrestrial"],
            multiple=True,
        ),
        ExtractionField("diet", prompt="Describe the primary diet"),
        ExtractionField(
            "conservation_status",
            options=["Endangered", "Vulnerable", "Least Concern"],
        ),
    ]

    result = extractor.extract("Orca", fields=fields)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()