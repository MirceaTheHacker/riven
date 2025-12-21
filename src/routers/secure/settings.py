from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ValidationError
from loguru import logger

from program.settings.manager import settings_manager
from program.settings.models import AppModel

from ..models.shared import MessageResponse


class SetSettings(BaseModel):
    key: str
    value: Any


router = APIRouter(
    prefix="/settings",
    tags=["settings"],
    responses={404: {"description": "Not found"}},
)


@router.get("/schema", operation_id="get_settings_schema")
async def get_settings_schema() -> dict[str, Any]:
    """
    Get the JSON schema for the settings.
    """
    try:
        # Use the class method for getting schema in Pydantic v2
        return settings_manager.settings.__class__.model_json_schema()
    except Exception as e:
        logger.exception(f"Error getting settings schema: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get settings schema: {str(e)}")


@router.get("/load", operation_id="load_settings")
async def load_settings() -> MessageResponse:
    try:
        settings_manager.load()
        return {
            "message": "Settings loaded!",
        }
    except Exception as e:
        logger.exception(f"Error loading settings: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to load settings: {str(e)}")


@router.post("/save", operation_id="save_settings")
async def save_settings() -> MessageResponse:
    try:
        settings_manager.save()
        return {
            "message": "Settings saved!",
        }
    except Exception as e:
        logger.exception(f"Error saving settings: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to save settings: {str(e)}")


@router.get("/get/all", operation_id="get_all_settings")
async def get_all_settings() -> AppModel:
    try:
        # Create a new instance to avoid Observable serialization issues
        # FastAPI will serialize this properly using Pydantic's model_dump_json
        settings_dict = settings_manager.settings.model_dump()
        new_settings = AppModel.model_validate(settings_dict)
        return new_settings
    except Exception as e:
        logger.exception(f"Error getting all settings: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get settings: {str(e)}")


@router.get("/get/{paths}", operation_id="get_settings")
async def get_settings(paths: str) -> dict[str, Any]:
    try:
        current_settings = settings_manager.settings.model_dump()
        data = {}
        for path in paths.split(","):
            keys = path.split(".")
            current_obj = current_settings

            for k in keys:
                if k not in current_obj:
                    continue
                current_obj = current_obj[k]

            data[path] = current_obj
        return data
    except Exception as e:
        logger.exception(f"Error getting settings for paths {paths}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get settings: {str(e)}")


@router.post("/set/all", operation_id="set_all_settings")
async def set_all_settings(new_settings: Dict[str, Any]) -> MessageResponse:
    current_settings = settings_manager.settings.model_dump()

    def update_settings(current_obj, new_obj):
        for key, value in new_obj.items():
            if isinstance(value, dict) and key in current_obj:
                update_settings(current_obj[key], value)
            else:
                current_obj[key] = value

    update_settings(current_settings, new_settings)

    # Validate and save the updated settings
    try:
        updated_settings = settings_manager.settings.model_validate(current_settings)
        settings_manager.load(settings_dict=updated_settings.model_dump())
        settings_manager.save()  # Ensure the changes are persisted
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {
        "message": "All settings updated successfully!",
    }


@router.post("/set", operation_id="set_settings")
async def set_settings(settings: List[SetSettings]) -> MessageResponse:
    current_settings = settings_manager.settings.model_dump()

    for setting in settings:
        keys = setting.key.split(".")
        current_obj = current_settings

        # Navigate to the last key's parent object, ensuring all keys exist.
        for k in keys[:-1]:
            if k not in current_obj:
                raise HTTPException(
                    status_code=400,
                    detail=f"Path '{'.'.join(keys[:-1])}' does not exist.",
                )
            current_obj = current_obj[k]

        # Ensure the final key exists before setting the value.
        if keys[-1] in current_obj:
            current_obj[keys[-1]] = setting.value
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Key '{keys[-1]}' does not exist in path '{'.'.join(keys[:-1])}'.",
            )

    # Validate and apply the updated settings to the AppModel instance
    try:
        updated_settings = settings_manager.settings.__class__(**current_settings)
        settings_manager.load(settings_dict=updated_settings.model_dump())
        settings_manager.save()  # Ensure the changes are persisted
    except ValidationError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to update settings: {str(e)}",
        ) from e

    return {"message": "Settings updated successfully."}
