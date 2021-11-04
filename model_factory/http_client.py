"""Library for making calls to GCP services."""
import base64
import json
import mimetypes
import time
from typing import Any, Mapping, Optional, Sequence

import google.auth
import google.auth.transport.requests
from googleapiclient import discovery
from immutabledict import immutabledict
from ratelimiter import RateLimiter
import requests

_DOCUMENT_AI_HOSTS = immutabledict(
    {'us': 'https://us-documentai.googleapis.com/'})
_RESOURCE_EXHAUSTED_ERROR_CODE = 429
_MIN_RETRY_INTERVAL_SECS = 1
_MAX_RETRY_INTERVAL_SECS = 120


def _extract_lro_name(response: requests.Response) -> str:
  """Extracts long-running operation name from the response."""
  try:
    response_json = response.json()
    if 'name' in response_json:
      return response_json['name']
    raise RuntimeError(response.text)
  except:
    raise RuntimeError(response.text)


class DocumentAIClient:
  """Library for calling Document AI API HTTP endpoint."""

  def __init__(self, project_number: str = '', region: str = 'us'):
    """Initializes a client library for calling GCP services.

    Args:
      project_number: Resource project number. If left empty, the project
        associated with the environment (Compute Engine) will be used.
      region: Region of GCP end points to use.
    """
    if region not in _DOCUMENT_AI_HOSTS:
      raise ValueError(f'Unsupprted region: {region}')
    self._host = _DOCUMENT_AI_HOSTS[region]
    self._credentials, project_id = google.auth.default()
    if project_number:
      self._project_number = project_number
    else:
      crm = discovery.build('cloudresourcemanager', 'v1')
      self._project_number = crm.projects().get(
          projectId=project_id).execute()['projectNumber']
    if region:
      self._location = f'projects/{self._project_number}/locations/{region}'
    else:
      self._location = f'projects/{self._project_number}'

  def _get_bearer_token(self):
    authentication_request = google.auth.transport.requests.Request()
    self._credentials.refresh(authentication_request)
    return self._credentials.token

  @RateLimiter(max_calls=1800, period=60)
  # https://cloud.google.com/document-ai/quotas
  def _call(self,
            url: str,
            post_data: Mapping[str, Any] = None,
            patch_data: Mapping[str, Any] = None) -> requests.Response:
    """Makes an authenticated HTTP call."""
    token = self._get_bearer_token()
    if post_data is not None:
      return requests.post(
          url,
          data=json.dumps(post_data),
          headers={'Authorization': f'Bearer {token}'})
    if patch_data is not None:
      return requests.patch(
          url,
          data=json.dumps(patch_data),
          headers={'Authorization': f'Bearer {token}'})
    return requests.get(url, headers={'Authorization': f'Bearer {token}'})

  def batch_process_documents(self, processor_name: str,
                              input_gcs_paths: Sequence[str],
                              output_gcs_path: str) -> str:
    """Starts a long-running operation to batch process documents.

    Args:
      processor_name: Name of the document processor in
        "projects/*/locations/*/processors/*" format.
      input_gcs_paths: A sequence of GCS paths to PDF files to process.
      output_gcs_path: A GCS path where the output is saved.

    Returns:
      A long-running operation name.

    Raises:
      ResourceExhaustedError: if fail to create LRO due to insufficient quota.
      RuntimeError: if encounter other error.
    """
    input_configs = []
    for path in input_gcs_paths:
      mime_type, _ = mimetypes.guess_type(path)
      if mime_type:
        input_configs.append({'gcs_source': path, 'mime_type': mime_type})
      else:
        raise ValueError('Invalid document: %s' % path)
    payload = {
        'input_configs': input_configs,
        'output_config': {
            'gcs_destination': output_gcs_path
        }
    }
    retry_interval = _MIN_RETRY_INTERVAL_SECS
    while True:
      response = self._call(
          f'{self._host}v1beta3/{processor_name}:batchProcess',
          post_data=payload)
      response_json = response.json()
      if 'name' in response_json:
        return response_json['name']
      if 'error' in response_json:
        if response_json['error']['code'] == _RESOURCE_EXHAUSTED_ERROR_CODE:
          time.sleep(retry_interval)
          retry_interval = min(retry_interval * 2, _MAX_RETRY_INTERVAL_SECS)
          continue
      raise RuntimeError(response.text)

  def process_document(self, processor_name: str, content: bytes,
                       mime_type: str) -> Mapping[str, Any]:
    """Processes a document.

    Args:
      processor_name: Name of the document processor in
        "projects/*/locations/*/processors/*" format.
      content: Raw document bytes.
      mime_type: Mime type of the document.

    Returns:
      A processed document.
    """
    payload = {
        'raw_document': {
            'content': base64.b64encode(content).decode(),
            'mime_type': mime_type,
        },
    }
    response = self._call(
        f'{self._host}v1beta3/{processor_name}:process', post_data=payload)
    try:
      return response.json()['document']
    except:
      raise RuntimeError(response.text)

  def get_operation(self, operation_name: str):
    """Gets a long-running operation.

    Args:
      operation_name: Name of the long-running operation. Format:
        "projects/*/locations/*/operations/*".

    Returns:
      A JSON object of the returned operation.
    """
    response = self._call(f'{self._host}v1beta3/{operation_name}')
    try:
      return response.json()
    except:
      raise RuntimeError(response.text)

  def create_processor(self, processor_type: str, display_name: str) -> str:
    """Creates a document processor.

    Args:
      processor_type: Type of the processor. For example,
        "FORM_PARSER_PROCESSOR", "CUSTOM_EXTRACTION_PROCESSOR",
        "CUSTOM_CLASSIFICATION_PROCESSOR".
      display_name: Name of the processor to display in the Google Cloud Console
        UI.

    Returns:
      A processor name.
    """
    payload = {
        'type': processor_type,
        'display_name': display_name,
    }
    response = self._call(
        f'{self._host}uiv1beta3/{self._location}/processors', post_data=payload)
    try:
      return response.json()['name']
    except:
      raise RuntimeError(response.text)

  def review_document(self, processor_name: str, document: Mapping[str,
                                                                   Any]) -> str:
    """Sends a document for human review.

    Args:
      processor_name: Name of the document processor in
        "projects/*/locations/*/processors/*" format.
      document: Document JSON object.

    Returns:
      A long-running operation name.
    """
    payload = {
        'inline_document': document,
    }
    response = self._call(
        f'{self._host}v1beta3/{processor_name}/humanReviewConfig:reviewDocument',
        post_data=payload)
    try:
      response_json = response.json()
      if 'name' in response_json:
        return response_json['name']
      raise RuntimeError(response.text)
    except:
      raise RuntimeError(response.text)

  def get_human_review_config(self, processor_name: str) -> Mapping[str, Any]:
    """Gets the HumanReviewConfig of a processor.

    Args:
      processor_name: Name of the document processor in
        "projects/*/locations/*/processors/*" format.

    Returns:
       HumanReviewConfig.
    """
    response = self._call(
        f'{self._host}uiv1beta3/{processor_name}/humanReviewConfig')
    try:
      return response.json()
    except:
      raise RuntimeError(response.text)

  def update_human_review_config(self, processor_name: str,
                                 config: Mapping[str, Any]) -> str:
    """Updates human review config.

    Args:
      processor_name: Name of the document processor in
        "projects/*/locations/*/processors/*" format.
      config: Human review config.

    Returns:
      A long-running operation name.
    """
    response = self._call(
        f'{self._host}uiv1beta3/{processor_name}/humanReviewConfig',
        patch_data=config)
    return _extract_lro_name(response)

  def update_labeling_schema(self, processor_name: str,
                             schema: Mapping[str, Any]) -> str:
    """Updates labeling schema of a processor's human review config.

    Args:
      processor_name: Name of the document processor in
        "projects/*/locations/*/processors/*" format.
      schema: Data labeling schema.

    Returns:
      A long-running operation name.
    """
    # Partial update is not supported yet.
    config = self.get_human_review_config(processor_name)
    config['labelingSchema'] = schema
    return self.update_human_review_config(processor_name, config)

  def train_processor_version(
      self,
      processor_name: str,
      display_name: str,
      schema: Mapping[str, Any],
      training_data_path: str,
      test_data_path: str,
      extraction_options: Optional[Mapping[str, Any]] = None,
      classification_options: Optional[Mapping[str, Any]] = None) -> str:
    """Trains a new processor version.

    Args:
      processor_name: Name of the document processor in
        "projects/*/locations/*/processors/*" format.
      display_name: Display name of the new processor version.
      schema: Processor schema.
      training_data_path: Path to the training data directory.
      test_data_path: Path to the test data directory.
      extraction_options: Options for training extraction processors.
      classification_options: Options for training classification processors.

    Returns:
      A long-running operation name.
    """
    payload = {
        'processorVersion': {
            'displayName': display_name,
        },
        'schema': schema,
        'inputData': {
            'trainingDocuments': {
                'gcsPrefix': {
                    'gcsUriPrefix': training_data_path
                }
            },
            'testDocuments': {
                'gcsPrefix': {
                    'gcsUriPrefix': test_data_path
                }
            }
        }
    }
    if extraction_options is not None:
      payload['customDocumentExtractionOptions'] = extraction_options
    if classification_options is not None:
      payload['customDocumentClassificationOptions'] = classification_options
    response = self._call(
        f'{self._host}uiv1beta3/{processor_name}/processorVersions:train',
        post_data=payload)
    return _extract_lro_name(response)

  def deploy_processor_version(self, processor_version_name: str):
    """Deploys a processor version.

    Args:
      processor_version_name: Name of the document processor version in
        "projects/*/locations/*/processors/*/processorVersion/*" format.

    Returns:
      A long-running operation name.
    """
    response = self._call(
        f'{self._host}uiv1beta3/{processor_version_name}:deploy', post_data={})
    return _extract_lro_name(response)

  def wait_for_lro(self,
                   lro_name: str,
                   poll_interval_seconds: int = 10) -> Mapping[str, Any]:
    """Waits until a LRO completes.

    Args:
      lro_name: Name of the long-running operation.
      poll_interval_seconds: The wait time before polling the LRO next time in
        second.

    Returns:
      Long-running operation.
    """
    while True:
      lro = self.get_operation(lro_name)
      if 'done' in lro:
        return lro
      time.sleep(poll_interval_seconds)

  def create_labeler_pool(self, display_name: str,
                          manager_emails: Sequence[str]) -> str:
    """Creates a labeler pool.

    Args:
      display_name: Display name of the labeler pool.
      manager_emails: Emails of labeler pool managers.

    Returns:
      Long-running operation name.
    """
    response = self._call(
        f'{self._host}uiv1beta3/{self._location}/labelerPools',
        post_data={
            'displayName': display_name,
            'manager_emails': manager_emails,
        })
    return _extract_lro_name(response)

  def list_labeler_pools(self, page_token: str = '') -> Mapping[str, Any]:
    """Lists labeler pools.

    Args:
      page_token: Optional page token. If empty, list the first page.

    Returns:
      A list of labeler pools.
    """
    response = self._call(
        f'{self._host}uiv1beta3/{self._location}/labelerPools?page_size=100&page_token={page_token}'
    )
    try:
      return response.json()
    except:
      raise RuntimeError(response.text)
