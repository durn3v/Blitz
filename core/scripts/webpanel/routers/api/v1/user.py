import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

from fastapi import APIRouter, HTTPException

from .schema.user import (
    UserListResponse,
    UserInfoResponse,
    AddUserInputBody,
    EditUserInputBody,
    UserUriResponse,
    AddBulkUsersInputBody,
    UsernamesRequest,
    BatchCreateUsersRequest,
    BatchCreateUsersResponse,
    BatchCreatedUser,
    BatchSkippedUser,
    BatchErrorEntry,
)
from .schema.response import DetailResponse
import cli_api

logger = logging.getLogger(__name__)

_SCRIPTS_DIR = Path(__file__).resolve().parents[4]
_db = None


def _get_db():
    global _db
    if _db is not None:
        return _db
    if str(_SCRIPTS_DIR) not in sys.path:
        sys.path.insert(0, str(_SCRIPTS_DIR))
    from db.database import db as database  # noqa: E402
    _db = database
    return _db


TRAFFIC_BYTES_PER_GB = 1073741824
USERNAME_PATTERN = re.compile(r"^[a-zA-Z0-9_]+$")
PASSWORD_PATTERN = re.compile(r"^[a-zA-Z0-9]{8,64}$")
CREATION_DATE_PATTERN = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")

router = APIRouter()


@router.get('/', response_model=UserListResponse)
async def list_users_api():
    """
    Get a list of all users.

    Returns:
        List of user dictionaries.
    Raises:
        HTTPException: if no users are found, or if an error occurs.
    """
    try:
        if res := cli_api.list_users():
            return res
        raise HTTPException(status_code=404, detail='No users found.')
    except Exception as e:
        raise HTTPException(status_code=400, detail=f'Error: {str(e)}')


@router.post('/', response_model=DetailResponse, status_code=201)
async def add_user_api(body: AddUserInputBody):
    try:
        cli_api.get_user(body.username)
        raise HTTPException(status_code=409,
                            detail=f"User '{body.username}' already exists.")
    except cli_api.CommandExecutionError:
        pass
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500,
                            detail=f"{str(e)}")

    try:
        cli_api.add_user(body.username, body.traffic_limit, body.expiration_days, body.password, body.creation_date, body.unlimited, body.note)
        return DetailResponse(detail=f'User {body.username} has been added.')
    except cli_api.CommandExecutionError as e:
        if "User already exists" in str(e):
            raise HTTPException(status_code=409,
                                detail=f"User '{body.username}' already exists.")
        raise HTTPException(status_code=400,
                            detail=f'Failed to add user {body.username}: {str(e)}')
    except cli_api.PasswordGenerationError as e:
        raise HTTPException(status_code=500,
                            detail=f"Failed to generate password for user '{body.username}': {str(e)}")
    except cli_api.InvalidInputError as e:
         raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500,
                            detail=f"An unexpected error occurred while adding user '{body.username}': {str(e)}")


@router.post('/bulk/', response_model=DetailResponse, status_code=201)
async def add_bulk_users_api(body: AddBulkUsersInputBody):
    """
    Add multiple users in bulk.
    """
    try:
        cli_api.bulk_user_add(
            traffic_gb=body.traffic_gb,
            expiration_days=body.expiration_days,
            count=body.count,
            prefix=body.prefix,
            start_number=body.start_number,
            unlimited=body.unlimited
        )
        return DetailResponse(detail=f"Successfully started adding {body.count} users with prefix '{body.prefix}'.")
    except cli_api.CommandExecutionError as e:
        raise HTTPException(status_code=400,
                            detail=f'Failed to add bulk users: {str(e)}')
    except Exception as e:
        raise HTTPException(status_code=500,
                            detail=f"An unexpected error occurred while adding bulk users: {str(e)}")


def _validate_batch_user(index: int, user) -> Tuple[Optional[str], Optional[str]]:
    """Return (username_lower, error_reason) — username_lower set when valid."""
    username = user.username
    if not USERNAME_PATTERN.match(username):
        return None, 'invalid_username'
    if not PASSWORD_PATTERN.match(user.password):
        return None, 'invalid_password'
    if user.expiration_days < 0:
        return None, 'invalid_expiration_days'
    if user.creation_date is not None:
        if not CREATION_DATE_PATTERN.match(user.creation_date):
            return None, 'invalid_creation_date'
        try:
            datetime.strptime(user.creation_date, '%Y-%m-%d')
        except ValueError:
            return None, 'invalid_creation_date'
    return username.lower(), None


def _build_batch_user_doc(user, username_lower: str) -> dict:
    doc = {
        '_id': username_lower,
        'password': user.password,
        'max_download_bytes': int(user.traffic_limit * TRAFFIC_BYTES_PER_GB),
        'expiration_days': user.expiration_days,
        'blocked': False,
        'unlimited_user': user.unlimited,
        'status': 'On-hold',
    }
    if user.creation_date:
        doc['account_creation_date'] = user.creation_date
    if user.note:
        doc['note'] = user.note
    return doc


@router.post('/batch-create', response_model=BatchCreateUsersResponse, status_code=201)
def batch_create_users_api(body: BatchCreateUsersRequest):
    """
    Create many users in one request with explicit usernames and passwords.
    Uses direct MongoDB insert_many (no subprocess per user).
    """
    from pymongo.errors import BulkWriteError

    db = _get_db()
    if db is None:
        raise HTTPException(
            status_code=500,
            detail='Database connection failed.',
        )

    started = time.monotonic()
    created: List[BatchCreatedUser] = []
    skipped: List[BatchSkippedUser] = []
    errors: List[BatchErrorEntry] = []

    candidates = []
    seen_in_request = set()

    for index, user in enumerate(body.users):
        username_lower, reason = _validate_batch_user(index, user)
        if reason:
            errors.append(BatchErrorEntry(
                index=index,
                username=user.username,
                reason=reason,
            ))
            continue

        if username_lower in seen_in_request:
            errors.append(BatchErrorEntry(
                index=index,
                username=user.username,
                reason='duplicate_username',
            ))
            continue

        seen_in_request.add(username_lower)
        candidates.append((index, user, username_lower))

    if candidates:
        candidate_ids = [username_lower for _, _, username_lower in candidates]
        try:
            existing_docs = db.collection.find(
                {'_id': {'$in': candidate_ids}},
                {'_id': 1},
            )
            existing_set = {doc['_id'] for doc in existing_docs}
        except Exception as e:
            logger.exception('batch-create: failed to query existing users')
            raise HTTPException(
                status_code=500,
                detail=f'Database query failed: {e}',
            ) from e

        users_to_insert = []
        pending_usernames = []
        for index, user, username_lower in candidates:
            if username_lower in existing_set:
                skipped.append(BatchSkippedUser(
                    username=username_lower,
                    reason='already_exists',
                ))
                continue
            users_to_insert.append(_build_batch_user_doc(user, username_lower))
            pending_usernames.append(username_lower)

        if users_to_insert:
            try:
                db.collection.insert_many(users_to_insert, ordered=False)
                created.extend(
                    BatchCreatedUser(username=u) for u in pending_usernames
                )
            except BulkWriteError as e:
                details = e.details or {}
                inserted = details.get('nInserted', 0)
                failed_indices = set()
                duplicate_indices = set()
                for write_error in details.get('writeErrors', []):
                    err_index = write_error.get('index')
                    if write_error.get('code') == 11000:
                        duplicate_indices.add(err_index)
                    else:
                        failed_indices.add(err_index)

                for i, username_lower in enumerate(pending_usernames):
                    if i in duplicate_indices:
                        skipped.append(BatchSkippedUser(
                            username=username_lower,
                            reason='already_exists',
                        ))
                    elif i not in failed_indices:
                        created.append(BatchCreatedUser(username=username_lower))

                if failed_indices:
                    logger.exception(
                        'batch-create: insert errors (inserted=%s)',
                        inserted,
                    )
                    raise HTTPException(
                        status_code=500,
                        detail=(
                            f'Database insert failed after inserting {inserted} '
                            f'of {len(users_to_insert)} users.'
                        ),
                    ) from e
            except Exception as e:
                logger.exception('batch-create: insert_many failed')
                raise HTTPException(
                    status_code=500,
                    detail=f'Database insert failed: {e}',
                ) from e

    duration = time.monotonic() - started
    logger.info(
        'batch-create: created=%s skipped=%s failed=%s duration=%.3fs',
        len(created),
        len(skipped),
        len(errors),
        duration,
    )

    return BatchCreateUsersResponse(
        created_count=len(created),
        skipped_count=len(skipped),
        failed_count=len(errors),
        created=created,
        skipped=skipped,
        errors=errors,
    )


@router.post('/uri/bulk', response_model=List[UserUriResponse])
async def show_multiple_user_uris_api(request: UsernamesRequest):
    """
    Get URI information for multiple users in a single request for efficiency.
    """
    if not request.usernames:
        return []
        
    try:
        uri_data_list = cli_api.show_user_uri_json(request.usernames)
        if not uri_data_list:
            raise HTTPException(status_code=404, detail='No URI data found for the provided users.')
        
        valid_responses = [data for data in uri_data_list if not data.get('error')]
        
        return valid_responses
    except cli_api.ScriptNotFoundError as e:
        raise HTTPException(status_code=500, detail=f'Server script error: {str(e)}')
    except cli_api.CommandExecutionError as e:
        raise HTTPException(status_code=400, detail=f'Error executing script: {str(e)}')
    except Exception as e:
        raise HTTPException(status_code=400, detail=f'Unexpected error: {str(e)}')


@router.post('/bulk-delete', response_model=DetailResponse)
async def bulk_remove_users_api(body: UsernamesRequest):
    if not body.usernames:
        raise HTTPException(status_code=400, detail="No usernames provided.")
    try:
        cli_api.kick_users_by_name(body.usernames)
        cli_api.traffic_status(display_output=False)
        cli_api.remove_users(body.usernames)
        return DetailResponse(detail=f'Users have been removed.')
    except Exception as e:
        raise HTTPException(status_code=400, detail=f'Error: {str(e)}')


@router.get('/{username}', response_model=UserInfoResponse)
async def get_user_api(username: str):
    """
    Get the details of a user.

    Args:
        username: The username of the user to get.

    Returns:
        A user dictionary.

    Raises:
        HTTPException: if the user is not found, or if an error occurs.
    """
    try:
        user_data = cli_api.get_user(username)
        if not user_data:
            raise HTTPException(status_code=404, detail=f'User {username} not found.')
        
        if '_id' in user_data:
            user_data['username'] = user_data.pop('_id')
            
        return user_data
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse user data from CLI: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'An unexpected error occurred: {str(e)}')


@router.patch('/{username}', response_model=DetailResponse)
async def edit_user_api(username: str, body: EditUserInputBody):
    """
    Edit a user's details.

    Args:
        username: The username of the user to edit.
        body: An instance of EditUserInputBody containing the new user details.

    Returns:
        A DetailResponse with a message indicating the user has been edited.

    Raises:
        HTTPException: if an error occurs while editing the user.
    """
    try:
        cli_api.kick_users_by_name([username])
        cli_api.traffic_status(display_output=False)
        cli_api.edit_user(username, body.new_username, body.new_password, body.new_traffic_limit, body.new_expiration_days,
                          body.renew_password, body.renew_creation_date, body.blocked, body.unlimited_ip, body.note)
        return DetailResponse(detail=f'User {username} has been edited.')
    except Exception as e:
        raise HTTPException(status_code=400, detail=f'Error: {str(e)}')


@router.delete('/{username}', response_model=DetailResponse)
async def remove_user_api(username: str):
    """
    Remove a user.

    Args:
        username: The username of the user to remove.

    Returns:
        A DetailResponse with a message indicating the user has been removed.

    Raises:
        HTTPException: 404 if the user is not found, 400 if another error occurs.
    """
    try:
        user = cli_api.get_user(username)
        if not user:
            raise HTTPException(status_code=404, detail=f'User {username} not found.')
        
        cli_api.kick_users_by_name([username])
        cli_api.traffic_status(display_output=False)
        cli_api.remove_users([username])
        return DetailResponse(detail=f'User {username} has been removed.')
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f'Error: {str(e)}')


@router.get('/{username}/reset', response_model=DetailResponse)
async def reset_user_api(username: str):
    """
    Resets a user.

    Args:
        username: The username of the user to reset.

    Returns:
        A DetailResponse with a message indicating the user has been reset.

    Raises:
        HTTPException: if an error occurs while resetting the user.
    """
    try:
        user = cli_api.get_user(username)
        if not user:
            raise HTTPException(status_code=404, detail=f'User {username} not found.')
        
        cli_api.reset_user(username)
        return DetailResponse(detail=f'User {username} has been reset.')
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f'Error: {str(e)}')

@router.get('/{username}/uri', response_model=UserUriResponse)
async def show_user_uri_api(username: str):
    """
    Get the URI information for a user in JSON format.

    Args:
        username: The username of the user.

    Returns:
        UserUriResponse: An object containing URI information for the user.

    Raises:
        HTTPException: 404 if the user is not found, 400 if another error occurs.
    """
    try:
        uri_data_list = cli_api.show_user_uri_json([username])
        if not uri_data_list:
            raise HTTPException(status_code=404, detail=f'URI for user {username} not found.')
        
        uri_data = uri_data_list[0]
        if uri_data.get('error'):
            raise HTTPException(status_code=404, detail=f"{uri_data['error']}")
        
        return UserUriResponse(**uri_data)
    except cli_api.ScriptNotFoundError as e:
        raise HTTPException(status_code=500, detail=f'Server script error: {str(e)}')
    except cli_api.CommandExecutionError as e:
        raise HTTPException(status_code=400, detail=f'Error executing script: {str(e)}')
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f'Unexpected error: {str(e)}')