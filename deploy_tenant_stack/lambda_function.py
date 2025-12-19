import boto3
import json
import logging
import time
import hashlib
import re
from typing import Dict, Any, Optional, List, Set

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
athena = boto3.client("athena")

# Production Configuration
PRODUCTION_BUCKET = "myrecruiter-picasso"
TENANTS_PREFIX = "tenants"
MAPPINGS_PREFIX = "mappings"
EMBED_PREFIX = "embed"
CLOUDFRONT_DOMAIN = "chat.myrecruiter.ai"

# Analytics Configuration
ATHENA_DATABASE = "picasso_analytics"
ATHENA_TABLE = "events"
ATHENA_OUTPUT_LOCATION = "s3://picasso-analytics/athena-results/"

def count_enabled_features(features: Dict[str, Any]) -> int:
    """Count enabled features, including nested ones like callout"""
    count = 0
    for value in features.values():
        if isinstance(value, bool) and value:
            count += 1
        elif isinstance(value, dict) and value.get("enabled") is True:
            count += 1
    return count


def slugify(text: str) -> str:
    """
    Convert text to URL-friendly slug for action chip IDs
    Examples:
        "Learn about Mentoring" -> "learn_about_mentoring"
        "Request Support for Your Family" -> "request_support_for_your_family"
        "Volunteer!" -> "volunteer"
        "FAQ's & Info" -> "faqs_info"
    """
    if not text:
        return ""

    # Convert to lowercase
    slug = text.lower()

    # Remove special characters except spaces and hyphens
    slug = re.sub(r'[^\w\s-]', '', slug)

    # Replace spaces and hyphens with underscores
    slug = re.sub(r'[-\s]+', '_', slug)

    # Remove leading/trailing underscores
    slug = slug.strip('_')

    return slug


def generate_chip_id(label: str, existing_ids: Set[str]) -> str:
    """
    Generate unique chip ID from label with collision detection
    Args:
        label: The action chip label text
        existing_ids: Set of already-used chip IDs
    Returns:
        Unique chip ID with numeric suffix if needed
    """
    base_id = slugify(label)

    if not base_id:
        base_id = "action_chip"

    chip_id = base_id
    counter = 2

    # Detect collisions and add numeric suffix
    while chip_id in existing_ids:
        chip_id = f"{base_id}_{counter}"
        counter += 1

    return chip_id


def transform_action_chips_array_to_dict(chips_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform action chips from array format to dictionary format

    Before (Bubble format):
    {
        "enabled": true,
        "default_chips": [
            {"label": "Learn about Mentoring", "value": "Tell me about..."}
        ]
    }

    After (Enhanced format):
    {
        "enabled": true,
        "default_chips": {
            "learn_about_mentoring": {
                "label": "Learn about Mentoring",
                "value": "Tell me about...",
                "target_branch": null  # Can be set in Config Builder
            }
        }
    }
    """
    if not chips_config:
        return chips_config

    # If default_chips doesn't exist or is not a list, return as-is
    default_chips = chips_config.get("default_chips")
    if not default_chips or not isinstance(default_chips, list):
        return chips_config

    # If it's already a dictionary, return as-is (backward compatibility)
    if isinstance(default_chips, dict):
        logger.info("Action chips already in dictionary format, skipping transformation")
        return chips_config

    # Transform array to dictionary
    logger.info(f"Transforming {len(default_chips)} action chips from array to dictionary format")

    transformed_chips = {}
    existing_ids: Set[str] = set()

    for chip in default_chips:
        if not isinstance(chip, dict):
            logger.warning(f"Skipping invalid chip (not a dict): {chip}")
            continue

        label = chip.get("label", "")
        if not label:
            logger.warning(f"Skipping chip with no label: {chip}")
            continue

        # Generate unique ID
        chip_id = generate_chip_id(label, existing_ids)
        existing_ids.add(chip_id)

        # Create enhanced chip object
        transformed_chips[chip_id] = {
            "label": label,
            "value": chip.get("value", label),
            "target_branch": None  # Will be set in Config Builder UI
        }

        logger.info(f"  Transformed chip: '{label}' -> ID: '{chip_id}'")

    # Return transformed config
    return {
        "enabled": chips_config.get("enabled", True),
        "max_display": chips_config.get("max_display"),
        "show_on_welcome": chips_config.get("show_on_welcome"),
        "default_chips": transformed_chips
    }


def lambda_handler(event, context):
    logger.info("🚀 PRODUCTION Deploy Lambda triggered with Universal Widget support")

    # Handle CORS preflight requests
    if event.get("httpMethod") == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type"
            },
            "body": ""
        }

    # Parse event body (from Bubble)
    try:
        raw_body = event.get("body", "{}")
        if isinstance(raw_body, str):
            bubble_data = json.loads(raw_body)
        else:
            bubble_data = raw_body
    except Exception as e:
        logger.error("❌ Failed to parse event body: %s", str(e))
        return _error("Invalid request body", details=str(e))

    # Extract tenant ID
    tenant_id = bubble_data.get("tenant_id")
    if not tenant_id:
        return _error("Missing required: tenant_id")

    # 🔒 SURGICAL FIX 1: Remove tenant_id from logs
    logger.info(f"🎯 PRODUCTION DEPLOYMENT initiated")
    logger.info(f"📥 Received {len(bubble_data)} fields from Bubble")

    try:
        # 1. VALIDATE BUBBLE INPUT - NEW VALIDATION STEP
        logger.info("🔍 Validating Bubble input...")
        validation_warnings = validate_bubble_input(bubble_data)
        if validation_warnings:
            logger.warning(f"⚠️ Validation warnings: {validation_warnings}")
        logger.info("✅ Input validation passed")
        
        # 2. GENERATE TENANT HASH
        logger.info("🔗 Generating tenant hash...")
        tenant_hash = generate_tenant_hash(tenant_id)
        # 🔒 SURGICAL FIX 2: Don't log the full hash
        logger.info(f"✅ Generated hash: {tenant_hash[:8]}...")
        
        # 3. TRANSFORM: Bubble flat structure → Picasso nested structure  
        logger.info("🔄 Starting Bubble→Picasso transformation...")
        picasso_config = transform_bubble_to_picasso_config(bubble_data)
        
        # 4. CREATE S3 FOLDER STRUCTURE
        tenant_folder = f"{TENANTS_PREFIX}/{tenant_id}/"
        
        # Create tenant folder
        s3.put_object(
            Bucket=PRODUCTION_BUCKET,
            Key=tenant_folder,
            Body=""
        )
        # 🔒 SURGICAL FIX 3: Remove tenant_id from logs
        logger.info(f"✅ Created tenant folder in S3")

        # 5. UPLOAD CONFIG FILE (CLEANED)
        config_key = f"{tenant_folder}{tenant_id}-config.json"
        # 🔒 SURGICAL FIX 4: Use hash-based config URL with consistent action pattern
        config_url = f"https://{CLOUDFRONT_DOMAIN}/Master_Function?action=get_config&t={tenant_hash}"
        
        # Clean the config before saving (removes empty/null values)
        cleaned_config = clean_config_output(picasso_config)
        config_content = json.dumps(cleaned_config, indent=2)
        
        s3.put_object(
            Bucket=PRODUCTION_BUCKET,
            Key=config_key,
            Body=config_content,
            ContentType="application/json",
            CacheControl="no-cache, must-revalidate"
        )
        # 🔒 SURGICAL FIX 5: Remove tenant_id from logs
        logger.info(f"✅ Uploaded config to S3")

        # 6. VERIFY CONFIG UPLOAD - NEW VERIFICATION STEP
        logger.info("🔍 Verifying config upload...")
        verify_config_upload(config_key, tenant_id)
        logger.info("✅ Config verification passed")

        # 7. STORE TENANT HASH MAPPING
        logger.info("💾 Storing tenant hash mapping...")
        store_tenant_mapping(tenant_id, tenant_hash)

        # 8. UPDATE ATHENA PARTITION PROJECTION (for analytics)
        logger.info("📊 Updating Athena partition projection...")
        update_athena_tenant_partition(tenant_id)

        # 9. GENERATE CLEAN EMBED CODE
        clean_embed_code = generate_clean_embed_code(tenant_hash)
        logger.info(f"✨ Generated clean embed code")

        # 🔒 SURGICAL FIX 6: REMOVE LEGACY EMBED SCRIPT GENERATION
        # No longer creating legacy embed scripts that expose tenant_id

        # 11. GENERATE IFRAME-AWARE EMBED CODES
        widget_url = f"https://{CLOUDFRONT_DOMAIN}/widget.js"

        # Simple embed (recommended for most users)
        embedded_code = f'<script src="{widget_url}" data-tenant="{tenant_hash}" async></script>'
        
        # Advanced embed with configuration
        advanced_embed_code = f'''<!-- Picasso Chat Widget (Iframe-based) -->
<script src="{widget_url}" data-tenant="{tenant_hash}" async></script>
<script>
// Optional: Pre-configure widget behavior
window.PicassoConfig = {{
  tenant: "{tenant_hash}",
  position: "bottom-right", // Options: bottom-right, bottom-left, top-right, top-left
  startOpen: false          // Set true to open widget on load
}};

// Optional: Use widget API after load
window.addEventListener('load', function() {{
  // PicassoWidget.open();   // Open chat
  // PicassoWidget.close();  // Close chat
  // PicassoWidget.toggle(); // Toggle chat
  // if (PicassoWidget.isOpen()) {{ /* ... */ }} // Check state
}});
</script>'''

        fullpage_code = clean_embed_code  # reuse existing clean embed script
        
        # 12. GENERATE HASHED EMBED SCRIPT (SECURE)
        hashed_embed_script = generate_hashed_embed_script(tenant_hash)
        hashed_embed_key = f"{EMBED_PREFIX}/{tenant_hash}.js"
        hashed_embed_url = f"https://{CLOUDFRONT_DOMAIN}/{hashed_embed_key}"

        s3.put_object(
            Bucket=PRODUCTION_BUCKET,
            Key=hashed_embed_key,
            Body=hashed_embed_script,
            ContentType="application/javascript",
            CacheControl="public, max-age=3600"
        )
        logger.info(f"✅ Generated hashed embed script")

        # 13. GENERATE DEPLOYMENT SUMMARY
        transformation_summary = {
            # 🔒 SURGICAL FIX 7: Remove tenant_id from summary
            "tenant_hash": tenant_hash,
            "deployment_timestamp": int(time.time()),
            "original_fields": len(bubble_data),
            "transformed_sections": len(cleaned_config),
            "features_enabled": count_enabled_features(cleaned_config.get("features", {})),
            "branding_properties": len(cleaned_config.get("branding", {})),
            "validation_warnings": validation_warnings,
            "s3_bucket": PRODUCTION_BUCKET,
            "cloudfront_domain": CLOUDFRONT_DOMAIN
        }

        # 🔒 SURGICAL FIX 8: SECURE SUCCESS RESPONSE - HASH ONLY
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type"
            },
            "body": json.dumps({
                "success": True,
                "message": "Configuration deployed successfully with iframe-based Universal Widget",
                # 🔒 ONLY RETURN HASH - NO TENANT_ID
                "tenant_hash": tenant_hash,

                # Primary embed options (NEW STRUCTURE)
                "embed_code": embedded_code,  # Simple one-liner
                "embed_code_advanced": advanced_embed_code,  # With configuration
                
                # Widget architecture info (NEW)
                "widget_info": {
                    "architecture": "iframe-based",
                    "isolation": "complete CSS/JS isolation from host page",
                    "container_sizes": {
                        "minimized": "90x90px (includes badge/callout space)",
                        "expanded_desktop": "360x640px",
                        "expanded_tablet": "responsive 40-60% viewport",
                        "expanded_mobile": "fullscreen"
                    },
                    "api_methods": {
                        "open": "PicassoWidget.open()",
                        "close": "PicassoWidget.close()",
                        "toggle": "PicassoWidget.toggle()",
                        "is_open": "PicassoWidget.isOpen()",
                        "on_event": "PicassoWidget.onEvent(callback)"
                    },
                    "events": [
                        "CHAT_OPENED",
                        "CHAT_CLOSED",
                        "MESSAGE_SENT",
                        "WIDGET_LOADED"
                    ],
                    "browser_support": "All modern browsers (Chrome, Firefox, Safari, Edge)",
                    "mobile_support": "iOS Safari, Chrome Mobile, Samsung Internet"
                },
                
                # Integration guide (NEW)
                "integration_guide": {
                    "basic": "Just paste the embed_code before </body>",
                    "positioning": "Widget appears bottom-right by default. Use PicassoConfig to change.",
                    "styling": "No CSS required. Widget handles all styling internally.",
                    "conflicts": "None. Iframe isolation prevents CSS/JS conflicts.",
                    "performance": "Async loading. Won't block page render.",
                    "gdpr": "Widget only loads when user interacts (GDPR compliant)"
                },

                # Legacy fields for backward compatibility
                "hashed_embed_url": hashed_embed_url,
                "widget_url": f"https://{CLOUDFRONT_DOMAIN}/widget.js",
                "embedded_code": embedded_code,  # Duplicate for legacy
                "fullpage_code": fullpage_code,
                "direct_embed_code": clean_embed_code,

                # 🔒 SURGICAL FIX 9: Hash-based API endpoints only
                "config_url": config_url,
                "update_url": f"https://{CLOUDFRONT_DOMAIN}/Master_Function?action=update_config&t={tenant_hash}",
                "health_url": f"https://{CLOUDFRONT_DOMAIN}/Master_Function?action=health_check&t={tenant_hash}",

                "transformation_summary": transformation_summary,
                "timestamp": context.aws_request_id
            })
        }

    except Exception as e:
        # 🔒 SURGICAL FIX 10: Remove tenant_id from error logs
        logger.exception(f"❌ PRODUCTION DEPLOYMENT FAILED")
        return _error("Deployment failed", details=str(e))


def validate_bubble_input(bubble_data: Dict[str, Any]) -> list:
    """Validate Bubble input before transformation"""
    warnings = []
    
    # Required fields
    required_fields = ["tenant_id"]
    for field in required_fields:
        if not bubble_data.get(field):
            raise ValueError(f"Missing required field: {field}")
    
    # Recommended fields for good user experience
    recommended_fields = {
        "chat_title": "Chat widget will show generic title",
        "primary_color": "Will use default blue color",
        "welcome_message": "Will use generic welcome message",
        "tone_prompt": "Will use generic assistant tone"
    }
    for field, warning_msg in recommended_fields.items():
        if not bubble_data.get(field):
            warnings.append(f"Missing {field}: {warning_msg}")
    
    # ✅ Validate color formats if provided
    color_fields = [
        "primary_color",
        "secondary_color",
        "font_color",
        "background_color",
        "logo_background_color",
        "avatar_background_color"
    ]
    for field in color_fields:
        if bubble_data.get(field):
            color_value = bubble_data[field].strip()
            if color_value and not is_valid_color(color_value):
                warnings.append(f"Invalid color format for {field}: {color_value}")
    
    # ✅ Validate boolean fields
    boolean_fields = ["uploads_enabled", "photo_uploads_enabled", "quick_help_enabled"]
    for field in boolean_fields:
        value = bubble_data.get(field)
        if value is not None and not is_valid_boolean(value):
            warnings.append(f"Invalid boolean value for {field}: {value}")
    
    logger.info(f"🔍 Validation complete: {len(warnings)} warnings")
    return warnings


def is_valid_color(color_value: str) -> bool:
    """Validate hex color format"""
    if not color_value:
        return False
    
    # Remove # if present
    color = color_value.strip().lstrip('#')
    
    # Check if it's a valid hex color (3 or 6 characters)
    if len(color) not in [3, 6]:
        return False
    
    try:
        int(color, 16)
        return True
    except ValueError:
        return False


def is_valid_boolean(value: Any) -> bool:
    """Validate boolean-like values from Bubble"""
    if isinstance(value, bool):
        return True
    if isinstance(value, str):
        return value.lower().strip() in ['<yes>', 'yes', 'true', '1', 'on', 'enabled', '<no>', 'no', 'false', '0', 'off', 'disabled']
    return False


def verify_config_upload(config_key: str, tenant_id: str):
    """Verify that the uploaded config is readable and valid"""
    try:
        response = s3.get_object(Bucket=PRODUCTION_BUCKET, Key=config_key)
        config_content = response["Body"].read()
        config_data = json.loads(config_content)
        
        # Basic validation checks
        if not config_data.get("tenant_id"):
            raise ValueError("Config missing tenant_id")
        
        if config_data.get("tenant_id") != tenant_id:
            raise ValueError("Config tenant_id mismatch")
        
        if not config_data.get("branding"):
            # 🔒 SURGICAL FIX 11: Remove tenant_id from logs
            logger.warning(f"⚠️ Config has no branding section")
        
        logger.info(f"✅ Config verification passed")
        
    except Exception as e:
        # 🔒 SURGICAL FIX 12: Remove tenant_id from logs
        logger.error(f"❌ Config verification failed: {str(e)}")
        raise


def clean_config_output(config):
    """Remove empty/null/meaningless values from config before saving to S3"""
    def is_empty_value(value):
        if value is None:
            return True
        if value == "":
            return True
        if value == {}:
            return True
        if value == []:
            return True
        if isinstance(value, str) and value.strip() == "":
            return True
        if isinstance(value, str) and value.strip().lower() == "null":
            return True
        return False
    
    def clean_recursive(obj):
        if isinstance(obj, dict):
            cleaned = {}
            for key, value in obj.items():
                if not is_empty_value(value):
                    cleaned_value = clean_recursive(value)
                    if not is_empty_value(cleaned_value):
                        cleaned[key] = cleaned_value
            return cleaned
        elif isinstance(obj, list):
            return [clean_recursive(item) for item in obj if not is_empty_value(item)]
        else:
            return obj
    
    return clean_recursive(config)

def transform_bubble_to_picasso_config(bubble_data: Dict[str, Any]) -> Dict[str, Any]:
    tenant_id = bubble_data.get("tenant_id")
    # 🔒 SURGICAL FIX 13: Remove tenant_id from logs
    logger.info(f"🔄 Transforming config")
    
    # Generate the hash here so we can include it in config
    tenant_hash = generate_tenant_hash(tenant_id)
    
   # CORE tenant information (ONLY include if Bubble sent them)
    core_config = {
        "tenant_id": tenant_id,
        "tenant_hash": tenant_hash
    }

    # Add optional core fields only if present
    if bubble_data.get("subscription_tier"):
        core_config["subscription_tier"] = bubble_data.get("subscription_tier")
    if bubble_data.get("chat_title"):
        core_config["chat_title"] = bubble_data.get("chat_title")
    if bubble_data.get("chat_subtitle"):
        core_config["chat_subtitle"] = bubble_data.get("chat_subtitle")
    if bubble_data.get("tone_prompt"):
        core_config["tone_prompt"] = bubble_data.get("tone_prompt")
    if bubble_data.get("welcome_message"):
        core_config["welcome_message"] = bubble_data.get("welcome_message")
    if bubble_data.get("callout_text"):
        core_config["callout_text"] = bubble_data.get("callout_text")
    
    # Always include version and timestamp
    core_config["version"] = "1.0"
    core_config["generated_at"] = int(time.time())
    
    # Transform branding (ONLY fields that Bubble sent, using your field names)
    branding = {}
    
    # Core colors
    if bubble_data.get("logo_background_color"):
        branding["logo_background_color"] = clean_color_value(bubble_data.get("logo_background_color"))
    if bubble_data.get("primary_color"):
        branding["primary_color"] = clean_color_value(bubble_data.get("primary_color"))
    if bubble_data.get("avatar_background_color"):
        branding["avatar_background_color"] = clean_color_value(bubble_data.get("avatar_background_color"))
    if bubble_data.get("secondary_color"):
        branding["secondary_color"] = clean_color_value(bubble_data.get("secondary_color"))
    if bubble_data.get("font_color"):
        branding["font_color"] = clean_color_value(bubble_data.get("font_color"))
    if bubble_data.get("background_color"):
        branding["background_color"] = clean_color_value(bubble_data.get("background_color"))
    
    # Bubble colors (using your actual field names)
    if bubble_data.get("bubble_user_bg"):
        branding["user_bubble_color"] = clean_color_value(bubble_data.get("bubble_user_bg"))
    if bubble_data.get("bubble_user_text"):
        branding["user_bubble_text_color"] = clean_color_value(bubble_data.get("bubble_user_text"))
    if bubble_data.get("bubble_bot_bg"):
        branding["bot_bubble_color"] = clean_color_value(bubble_data.get("bubble_bot_bg"))
    if bubble_data.get("bubble_bot_text"):
        branding["bot_bubble_text_color"] = clean_color_value(bubble_data.get("bubble_bot_text"))
    
    # Header colors (using your actual field names)
    if bubble_data.get("title_bar_color"):
        branding["header_background_color"] = clean_color_value(bubble_data.get("title_bar_color"))
    if bubble_data.get("title_color"):
        branding["header_text_color"] = clean_color_value(bubble_data.get("title_color"))
    
    # Header subtitle color
    if bubble_data.get("header_subtitle_color"):
        branding["header_subtitle_color"] = clean_color_value(bubble_data.get("header_subtitle_color"))
    
    # Chat title color mapping for FOS
    if bubble_data.get("chat_title_color"):
        branding["chat_title_color"] = clean_color_value(bubble_data.get("chat_title_color"))
    
    # Widget colors (using your actual field names)
    if bubble_data.get("chat_toggle_icon_color"):
        branding["widget_icon_color"] = clean_color_value(bubble_data.get("chat_toggle_icon_color"))
    if bubble_data.get("chat_toggle_background_color"):
        branding["widget_background_color"] = clean_color_value(bubble_data.get("chat_toggle_background_color"))
    
    # Widget icon color direct mapping for FOS
    if bubble_data.get("widget_icon_color"):
        branding["widget_icon_color"] = clean_color_value(bubble_data.get("widget_icon_color"))
    
    # Typography
    if bubble_data.get("font_family"):
        branding["font_family"] = clean_string_value(bubble_data.get("font_family"))
    if bubble_data.get("font_size_base"):
        branding["font_size_base"] = clean_string_value(bubble_data.get("font_size_base"))
    if bubble_data.get("font_size_heading"):
        branding["font_size_heading"] = clean_string_value(bubble_data.get("font_size_heading"))
    if bubble_data.get("font_weight"):
        branding["font_weight"] = clean_string_value(bubble_data.get("font_weight"))
    if bubble_data.get("line_height"):
        branding["line_height"] = clean_string_value(bubble_data.get("line_height"))
    
    # Layout
    if bubble_data.get("border_radius"):
        branding["border_radius"] = clean_string_value(bubble_data.get("border_radius"))
    if bubble_data.get("chat_position"):
        branding["chat_position"] = clean_string_value(bubble_data.get("chat_position"))
    if bubble_data.get("avatar_shape"):
        branding["avatar_shape"] = clean_string_value(bubble_data.get("avatar_shape"))
    
    # Assets
    if bubble_data.get("logo_url"):
        branding["logo_url"] = bubble_data.get("logo_url")
    if bubble_data.get("avatar_url"):
        branding["avatar_url"] = bubble_data.get("avatar_url")
    if bubble_data.get("company_logo_url"):
        branding["company_logo_url"] = bubble_data.get("company_logo_url")

    # Logo/Avatar Backgrounds
    if bubble_data.get("logo_background_color"):
        branding["logo_background_color"] = clean_color_value(bubble_data.get("logo_background_color"))
    if bubble_data.get("avatar_background_color"):
        branding["avatar_background_color"] = clean_color_value(bubble_data.get("avatar_background_color"))
    
    # Links
    if bubble_data.get("link_color"):
        branding["link_color"] = clean_color_value(bubble_data.get("link_color"))
    
    # Transform features (ONLY include if Bubble sent them)
    features = {}
    
    feature_mappings = {
        "uploads": "uploads_enabled",
        "photo_uploads": "photo_uploads_enabled",
        "conversational_forms": "conversational_forms_enabled",  # Form system feature flag
        "sms": "sms_enabled",
        "voice_input": "voice_input",
        "webchat": "webchat_enabled",
        "qr": "qr_enabled",
        "bedrock_kb": "bedrockKB_enabled",
        "ats": "ats_enabled",
        "interview_scheduling": "interview_scheduling_enabled"
    }
    
    for config_key, bubble_key in feature_mappings.items():
        if bubble_data.get(bubble_key) is not None:
            features[config_key] = parse_bubble_boolean(bubble_data.get(bubble_key))
    
    # Enhanced callout support for nested structure
    # Only create callout config if Bubble sends callout fields
    callout_fields_sent = any([
        bubble_data.get("callout_enabled") is not None,
        bubble_data.get("callout_text"),
        bubble_data.get("callout_delay"),
        bubble_data.get("callout_auto_dismiss") is not None,
        bubble_data.get("callout_dismiss_timeout")
    ])
    
    if callout_fields_sent:
        callout_config = {}
        
        if bubble_data.get("callout_enabled") is not None:
            callout_config["enabled"] = parse_bubble_boolean(bubble_data.get("callout_enabled"))
        if bubble_data.get("callout_text"):
            callout_config["text"] = bubble_data.get("callout_text")
        if bubble_data.get("callout_delay"):
            callout_config["delay"] = bubble_data.get("callout_delay")
        if bubble_data.get("callout_auto_dismiss") is not None:
            callout_config["auto_dismiss"] = parse_bubble_boolean(bubble_data.get("callout_auto_dismiss"))
        if bubble_data.get("callout_dismiss_timeout"):
            callout_config["dismiss_timeout"] = bubble_data.get("callout_dismiss_timeout")
        
        features["callout"] = callout_config
    
    # Quick help (ONLY include if Bubble sent them)
    quick_help = {}
    if bubble_data.get("quick_help_enabled") is not None:
        quick_help["enabled"] = parse_bubble_boolean(bubble_data.get("quick_help_enabled"))
    if bubble_data.get("quick_help_title"):
        quick_help["title"] = bubble_data.get("quick_help_title")
    if bubble_data.get("quick_help_toggle_text"):
        quick_help["toggle_text"] = bubble_data.get("quick_help_toggle_text")
    if bubble_data.get("quick_help_close_after") is not None:
        quick_help["close_after_selection"] = parse_bubble_boolean(bubble_data.get("quick_help_close_after"))
    if bubble_data.get("quick_help_prompts"):
        quick_help["prompts"] = bubble_data.get("quick_help_prompts")
    
    # Action chips (ONLY include if Bubble sent them)
    # Now with array→dictionary transformation for enhanced routing
    action_chips = {}
    if bubble_data.get("action_chips_enabled") is not None:
        action_chips["enabled"] = parse_bubble_boolean(bubble_data.get("action_chips_enabled"))
    if bubble_data.get("action_chips_max"):
        action_chips["max_display"] = bubble_data.get("action_chips_max")
    if bubble_data.get("action_chips_on_welcome") is not None:
        action_chips["show_on_welcome"] = parse_bubble_boolean(bubble_data.get("action_chips_on_welcome"))
    if bubble_data.get("action_chips_list"):
        action_chips["default_chips"] = bubble_data.get("action_chips_list")

    # Transform action chips from array to dictionary format (if needed)
    if action_chips:
        action_chips = transform_action_chips_array_to_dict(action_chips)
    
    # Widget behavior (ONLY include if Bubble sent them)
    widget_behavior = {}
    if bubble_data.get("start_open") is not None:
        widget_behavior["start_open"] = parse_bubble_boolean(bubble_data.get("start_open"))
    if bubble_data.get("remember_state") is not None:
        widget_behavior["remember_state"] = parse_bubble_boolean(bubble_data.get("remember_state"))
    if bubble_data.get("auto_open_delay") is not None:
        widget_behavior["auto_open_delay"] = bubble_data.get("auto_open_delay")
    
    # AWS configuration (ONLY include if Bubble sent them)
    aws_config = {}
    aws_mappings = {
        "knowledge_base_id": "knowledge_base_id",
        "aws_region": "aws_region",
        "bot_id": "bot_id",
        "bot_alias_id": "bot_alias_id"
    }
    
    for config_key, bubble_key in aws_mappings.items():
        if bubble_data.get(bubble_key):
            aws_config[config_key] = bubble_data.get(bubble_key)
    
    # Build final config (ONLY include sections that have content)
    transformed_config = core_config
    
    if branding:
        transformed_config["branding"] = branding
    if features:
        transformed_config["features"] = features
    if quick_help:
        transformed_config["quick_help"] = quick_help
    if action_chips:
        transformed_config["action_chips"] = action_chips
    if widget_behavior:
        transformed_config["widget_behavior"] = widget_behavior
    if aws_config:
        transformed_config["aws"] = aws_config

    # ALWAYS include form system structure (empty by default)
    # Web Config Builder will populate these when customer uses the feature
    # Feature flag in features.conversational_forms controls billing/access
    transformed_config["programs"] = {}
    transformed_config["conversational_forms"] = {}
    transformed_config["cta_definitions"] = {}
    transformed_config["conversation_branches"] = {}

    # CTA settings with fallback branch support (new routing feature)
    transformed_config["cta_settings"] = {
        "fallback_branch": None  # Will be set in Config Builder UI
    }

    # Always include metadata for debugging
    transformed_config["metadata"] = {
        "transformation_version": "1.0",
        "original_field_count": len(bubble_data),
        "deployment_timestamp": int(time.time()),
        "s3_bucket": PRODUCTION_BUCKET,
        "cloudfront_domain": CLOUDFRONT_DOMAIN
    }

    return transformed_config


def parse_bubble_boolean(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower().strip() in ['<yes>', 'yes', 'true', '1', 'on', 'enabled']
    return bool(value)


def clean_color_value(value: str) -> str:
    if not value or not isinstance(value, str):
        return ""
    
    cleaned = value.strip().replace('\n', '').replace('\r', '').lstrip('#')

    # Check valid hex: exactly 6 characters
    if len(cleaned) == 6:
        try:
            int(cleaned, 16)
            return f"#{cleaned.upper()}"
        except ValueError:
            return ""
    
    return ""  # If it's not valid 6-digit hex, return empty


def clean_string_value(value: str) -> str:
    if not value:
        return ""
    cleaned = value.strip()
    if cleaned.lower() == "null":
        return ""
    return cleaned


def generate_tenant_hash(tenant_id: str) -> str:
    """Generate a clean, professional hash for tenant ID"""
    salt = "picasso-2024-universal-widget"
    hash_input = f"{tenant_id}{salt}".encode('utf-8')
    full_hash = hashlib.sha256(hash_input).hexdigest()
    short_hash = full_hash[:12]
    prefix = tenant_id[:2].lower()
    return f"{prefix}{short_hash}"


def store_tenant_mapping(tenant_id: str, tenant_hash: str):
    """Store tenant hash mapping in S3 for lookup"""
    mapping_data = {
        "tenant_id": tenant_id,
        "tenant_hash": tenant_hash,
        "created_at": int(time.time()),
        "created_by": "deploy_lambda",
        "version": "1.0"
    }
    
    mapping_key = f"{MAPPINGS_PREFIX}/{tenant_hash}.json"
    
    try:
        s3.put_object(
            Bucket=PRODUCTION_BUCKET,
            Key=mapping_key,
            Body=json.dumps(mapping_data, indent=2),
            ContentType="application/json",
            CacheControl="public, max-age=86400"
        )
        
        logger.info(f"✅ Stored mapping in S3")
        
    except Exception as e:
        logger.error(f"❌ Failed to store tenant mapping: {str(e)}")
        raise


def update_athena_tenant_partition(tenant_id: str):
    """
    Add tenant_id to Athena table's partition projection enum.
    This ensures analytics queries can find data for new tenants immediately.

    The table uses partition projection with tenant_id as an enum type.
    New tenants must be added to the enum list for their data to be queryable.
    """
    try:
        # Get current table properties
        response = athena.start_query_execution(
            QueryString=f"SHOW TBLPROPERTIES {ATHENA_DATABASE}.{ATHENA_TABLE}",
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT_LOCATION}
        )
        query_id = response['QueryExecutionId']

        # Wait for query to complete (max 10 seconds)
        for _ in range(20):
            time.sleep(0.5)
            status = athena.get_query_execution(QueryExecutionId=query_id)
            state = status['QueryExecution']['Status']['State']
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break

        if state != 'SUCCEEDED':
            logger.warning(f"⚠️ Could not get Athena table properties: {state}")
            return

        # Get results
        results = athena.get_query_results(QueryExecutionId=query_id)

        # Find current tenant_id enum values
        # SHOW TBLPROPERTIES returns rows with "name\tvalue" in a single column
        current_tenants = set()
        for row in results.get('ResultSet', {}).get('Rows', []):
            data = row.get('Data', [])
            if len(data) >= 1:
                cell_value = data[0].get('VarCharValue', '')
                # Handle format: "projection.tenant_id.values\tVAL1,VAL2,VAL3"
                if 'projection.tenant_id.values' in cell_value:
                    parts = cell_value.split('\t')
                    if len(parts) >= 2:
                        current_tenants = set(parts[1].split(','))
                    break

        # Check if tenant already exists
        if tenant_id in current_tenants:
            logger.info(f"✅ Tenant already in Athena partition projection")
            return

        # Add new tenant to the list
        current_tenants.add(tenant_id)
        new_tenant_list = ','.join(sorted(current_tenants))

        # Update table properties
        alter_query = f"""
            ALTER TABLE {ATHENA_DATABASE}.{ATHENA_TABLE}
            SET TBLPROPERTIES (
                'projection.tenant_id.values' = '{new_tenant_list}'
            )
        """

        response = athena.start_query_execution(
            QueryString=alter_query,
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT_LOCATION}
        )
        alter_query_id = response['QueryExecutionId']

        # Wait for alter to complete
        for _ in range(20):
            time.sleep(0.5)
            status = athena.get_query_execution(QueryExecutionId=alter_query_id)
            state = status['QueryExecution']['Status']['State']
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break

        if state == 'SUCCEEDED':
            logger.info(f"✅ Added tenant to Athena partition projection")
        else:
            error_msg = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
            logger.warning(f"⚠️ Failed to update Athena partition projection: {error_msg}")

    except Exception as e:
        # Don't fail deployment if Athena update fails - analytics is non-critical
        logger.warning(f"⚠️ Could not update Athena partition projection: {str(e)}")


def generate_clean_embed_code(tenant_hash: str) -> str:
    """Generate clean, professional embed code using universal widget"""
    return f"""<!-- Picasso Chat Widget -->
<script src="https://{CLOUDFRONT_DOMAIN}/widget.js" 
        data-tenant="{tenant_hash}" 
        async>
</script>"""


def generate_hashed_embed_script(tenant_hash: str) -> str:
    """Generate secure embed script - iframe only (no CSS needed)"""
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())
    
    script = f"""// Picasso Chat Widget - Hash: {tenant_hash}
// Generated: {timestamp}
// Secure Iframe Widget

(function() {{
    if (window.PicassoWidgetLoaded) return;
    window.PicassoWidgetLoaded = true;
    
    // Load iframe widget (no CSS needed)
    var script = document.createElement('script');
    script.src = 'https://{CLOUDFRONT_DOMAIN}/widget.js';
    script.setAttribute('data-tenant', '{tenant_hash}');
    script.async = true;
    document.head.appendChild(script);
}})();"""
    
    return script


def _error(message: str, tenant_id: Optional[str] = None, details: Optional[str] = None):
    """Standardized error response"""
    # 🔒 SURGICAL FIX 14: Remove tenant_id from error responses
    return {
        "statusCode": 500,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type"
        },
        "body": json.dumps({
            "success": False,
            "error": message,
            "details": details or "No additional context"
        })
    }