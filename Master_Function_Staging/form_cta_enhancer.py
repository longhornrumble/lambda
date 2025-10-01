"""
Form CTA Enhancement Module for HTTP Mode
Application-layer CTA injection following AWS best practices (v5 implementation)
"""

import json
import logging
import boto3
import os
import time
from typing import Dict, Any, Optional, List

logger = logging.getLogger()
s3_client = boto3.client('s3')

# Cache for tenant configurations
config_cache = {}
# Cache for hash-to-ID mappings
hash_to_id_cache = {}

def resolve_tenant_hash(tenant_hash: str) -> Optional[str]:
    """Resolve tenant hash to tenant ID"""

    # Check cache first
    if tenant_hash in hash_to_id_cache:
        cache_age = time.time() - hash_to_id_cache[tenant_hash].get('timestamp', 0)
        if cache_age < 300:  # 5-minute cache
            tenant_id = hash_to_id_cache[tenant_hash]['tenant_id']
            logger.info(f"Using cached tenant_id for {tenant_hash[:8]}...")
            return tenant_id

    try:
        # Load mapping from S3
        bucket = os.environ.get('S3_CONFIG_BUCKET', 'myrecruiter-picasso')
        mapping_key = f"mappings/{tenant_hash}.json"

        response = s3_client.get_object(Bucket=bucket, Key=mapping_key)
        mapping_data = json.loads(response['Body'].read())

        tenant_id = mapping_data.get('tenant_id')
        if not tenant_id:
            logger.error(f"No tenant_id found in mapping for {tenant_hash}")
            return None

        # Cache the mapping
        hash_to_id_cache[tenant_hash] = {
            'tenant_id': tenant_id,
            'timestamp': time.time()
        }

        logger.info(f"Resolved {tenant_hash[:8]}... to tenant_id: {tenant_id}")
        return tenant_id

    except Exception as e:
        logger.error(f"Error resolving tenant hash {tenant_hash}: {e}")
        return None

def load_tenant_config(tenant_hash: str) -> Dict[str, Any]:
    """Load tenant configuration from S3 with caching"""

    # Check cache first
    if tenant_hash in config_cache:
        logger.info(f"Using cached config for tenant {tenant_hash[:8]}...")
        return config_cache[tenant_hash]

    try:
        # First resolve hash to ID
        tenant_id = resolve_tenant_hash(tenant_hash)
        if not tenant_id:
            logger.error(f"Could not resolve tenant hash to ID for {tenant_hash[:8]}...")
            return {
                'conversational_forms': {},
                'form_settings': {}
            }

        bucket = os.environ.get('S3_CONFIG_BUCKET', 'myrecruiter-picasso')
        key = f"tenants/{tenant_id}/{tenant_id}-config.json"

        response = s3_client.get_object(Bucket=bucket, Key=key)
        config = json.loads(response['Body'].read())

        # PHASE 1B: Cache the config with conversation_branches and cta_definitions
        config_cache[tenant_hash] = {
            'conversational_forms': config.get('conversational_forms', {}),
            'form_settings': config.get('form_settings', {}),
            'conversation_branches': config.get('conversation_branches', {}),  # PHASE 3
            'cta_definitions': config.get('cta_definitions', {})  # PHASE 3
        }

        logger.info(f"Loaded tenant config for {tenant_hash[:8]}... with {len(config.get('conversational_forms', {}))} forms, {len(config.get('conversation_branches', {}))} branches, {len(config.get('cta_definitions', {}))} CTAs")
        return config_cache[tenant_hash]

    except Exception as e:
        logger.error(f"Error loading tenant config: {e}")
        return {
            'conversational_forms': {},
            'form_settings': {}
        }

def should_trigger_form(user_message: str, forms: Dict[str, Any], readiness_score: float = 0.0) -> Optional[Dict[str, Any]]:
    """
    Determine if a form CTA should be triggered based on:
    1. Readiness score threshold (0.8+)
    2. Trigger phrase detection
    3. Form enabled status
    """

    # Check readiness threshold
    threshold = 0.8
    if readiness_score < threshold:
        logger.info(f"Readiness score {readiness_score} below threshold {threshold}")
        return None

    # Check for trigger phrases
    message_lower = user_message.lower()

    for form_id, form in forms.items():
        if not form.get('enabled', True):
            continue

        trigger_phrases = form.get('trigger_phrases', [])
        for phrase in trigger_phrases:
            if phrase.lower() in message_lower:
                logger.info(f"Form trigger detected: {form_id} via phrase '{phrase}'")
                return form

    return None

def calculate_readiness_score(conversation_history: List[Dict[str, str]], user_message: str = '') -> float:
    """
    Calculate readiness score based on conversation context
    Matches Bedrock_Streaming_Handler's algorithm
    """

    if not conversation_history:
        return 0.0

    score = 0.0
    message_count = len(conversation_history)

    # Message depth contributes to readiness (matching JS logic)
    if message_count >= 2:
        score += 0.2
    if message_count >= 4:
        score += 0.2
    if message_count >= 6:
        score += 0.1

    # Strong intent signals
    action_intents = ['volunteer', 'help', 'donate', 'join', 'support', 'contribute']
    latest_message = user_message or (conversation_history[-1].get('user', '') if conversation_history else '')

    if any(intent in latest_message.lower() for intent in action_intents):
        score += 0.3

    # Topic engagement - check for mentioned topics
    topic_patterns = ['program', 'service', 'offering', 'opportunity',
                     'requirement', 'age', 'commitment', 'qualification', 'eligible',
                     'process', 'how to', 'steps', 'apply', 'sign up',
                     'impact', 'help', 'difference', 'change', 'benefit',
                     'organization', 'mission', 'about us', 'who we are']

    topics_mentioned = sum(1 for topic in topic_patterns if topic in latest_message.lower())
    if topics_mentioned > 0:
        score += min(0.2, topics_mentioned * 0.05)

    # Conversation length (characters) - approximate from history
    conversation_text = ' '.join([msg.get('user', '') + msg.get('assistant', '')
                                 for msg in conversation_history])
    if len(conversation_text) > 500:
        score += 0.1

    # Cap at 1.0
    total_score = min(score, 1.0)
    logger.info(f"Readiness score: {total_score} (messages: {message_count})")

    return total_score

def create_form_cta_card(form: Dict[str, Any]) -> Dict[str, Any]:
    """Create a form CTA card structure for the frontend"""

    return {
        "type": "form_cta",
        "title": form.get("title", "Application Form"),
        "description": form.get("description", "Complete our quick form"),
        "ctaText": form.get("cta_text", "Would you like to apply?"),
        "formId": form.get("form_id"),
        "triggerForm": True,
        "style": "primary",  # Match Bedrock_Streaming_Handler
        "fields": form.get("fields", [])
    }

def filter_completed_forms(cards: List[Dict[str, Any]], completed_forms: List[str]) -> List[Dict[str, Any]]:
    """
    PHASE 2: Filter out CTAs for forms already completed
    Implements parity with Bedrock_Streaming_Handler filtering logic
    """
    if not completed_forms:
        return cards  # No filtering needed

    filtered = []
    for card in cards:
        form_id = card.get('formId') or card.get('form_id')

        if form_id and form_id in completed_forms:
            logger.info(f"[Phase 2] Filtering completed form CTA: {form_id}")
            continue

        filtered.append(card)

    logger.info(f"[Phase 2] Filtered {len(cards)} cards -> {len(filtered)} cards (removed {len(cards) - len(filtered)} completed forms)")
    return filtered

def detect_conversation_branch(
    response_text: str,
    user_message: str,
    config: Dict[str, Any],
    completed_forms: List[str] = None
) -> Optional[Dict[str, Any]]:
    """
    PHASE 3: Detect conversation branch based on response content
    Matches response to conversation_branches configuration
    Ported from response_enhancer.js
    """
    completed_forms = completed_forms or []
    conversation_branches = config.get('conversation_branches', {})
    cta_definitions = config.get('cta_definitions', {})

    if not conversation_branches or not cta_definitions:
        return None

    # Check if user is engaged/interested
    import re
    engaged_pattern = r'\b(tell me|more|interested|how|what|when|where|apply|volunteer|help|can i|do you|does)\b'
    if not re.search(engaged_pattern, user_message, re.IGNORECASE):
        logger.info('[Phase 3] User not engaged enough for CTAs')
        return None

    # Priority order for branch detection (broader topics first)
    branch_priority = [
        'program_exploration',
        'volunteer_interest',
        'requirements_discussion',
        'lovebox_discussion',
        'daretodream_discussion'
    ]

    # Check branches in priority order
    for branch_name in branch_priority:
        branch = conversation_branches.get(branch_name)
        if not branch or not branch.get('detection_keywords'):
            continue

        detection_keywords = branch.get('detection_keywords', [])
        if not isinstance(detection_keywords, list):
            continue

        # Check if any keywords match the response
        response_lower = response_text.lower()
        matches = any(keyword.lower() in response_lower for keyword in detection_keywords)

        if matches:
            logger.info(f"[Phase 3] Detected branch: {branch_name}")

            # Build CTA array from branch configuration
            ctas = []

            # Add primary CTA if defined and not completed
            available_ctas = branch.get('available_ctas', {})
            if available_ctas.get('primary'):
                primary_cta_id = available_ctas['primary']
                primary_cta = cta_definitions.get(primary_cta_id)

                if primary_cta:
                    # Check if this is a form CTA
                    is_form_cta = (
                        primary_cta.get('action') in ['start_form', 'form_trigger'] or
                        primary_cta.get('type') == 'form_cta'
                    )

                    if is_form_cta:
                        # Extract program from CTA
                        program = primary_cta.get('program') or primary_cta.get('program_id')

                        if not program:
                            # Map formIds to programs
                            form_id = primary_cta.get('formId') or primary_cta.get('form_id')
                            if form_id == 'lb_apply':
                                program = 'lovebox'
                            elif form_id == 'dd_apply':
                                program = 'daretodream'
                            elif form_id in ['volunteer_apply', 'volunteer_general']:
                                # Generic volunteer form - check branch context AND response content
                                if branch_name == 'lovebox_discussion' or 'love box' in response_lower:
                                    program = 'lovebox'
                                elif branch_name == 'daretodream_discussion' or 'dare to dream' in response_lower:
                                    program = 'daretodream'

                        # Filter if user has completed this program
                        if program and program in completed_forms:
                            logger.info(f"[Phase 3] 🚫 Filtering primary CTA for completed program: {program}")
                        else:
                            logger.info(f"[Phase 3] ✅ Adding primary CTA - program: {program or 'none'}")
                            ctas.append({
                                **primary_cta,
                                'id': primary_cta_id
                            })
                    else:
                        # Not a form CTA, always show
                        ctas.append({
                            **primary_cta,
                            'id': primary_cta_id
                        })

            # Add secondary CTAs if user seems engaged (message > 20 chars)
            if available_ctas.get('secondary') and len(user_message) > 20:
                for cta_id in available_ctas['secondary']:
                    cta = cta_definitions.get(cta_id)
                    if not cta:
                        continue

                    # Check if this is a form CTA
                    is_form_cta = (
                        cta.get('action') in ['start_form', 'form_trigger'] or
                        cta.get('type') == 'form_cta'
                    )

                    if is_form_cta:
                        # Extract program from CTA
                        program = cta.get('program') or cta.get('program_id')

                        if not program:
                            # Map formIds to programs
                            form_id = cta.get('formId') or cta.get('form_id')
                            if form_id == 'lb_apply':
                                program = 'lovebox'
                            elif form_id == 'dd_apply':
                                program = 'daretodream'
                            elif form_id in ['volunteer_apply', 'volunteer_general']:
                                # Generic volunteer form - check branch context
                                if branch_name == 'lovebox_discussion' or 'love box' in response_lower:
                                    program = 'lovebox'
                                elif branch_name == 'daretodream_discussion' or 'dare to dream' in response_lower:
                                    program = 'daretodream'

                        # Filter if user has completed this program
                        if program and program in completed_forms:
                            logger.info(f"[Phase 3] 🚫 Filtering secondary CTA for completed program: {program}")
                        else:
                            ctas.append({
                                **cta,
                                'id': cta_id
                            })
                    else:
                        # Not a form CTA, always show
                        ctas.append({
                            **cta,
                            'id': cta_id
                        })

            # Return max 3 CTAs for clarity
            return {
                'branch': branch_name,
                'ctas': ctas[:3]
            }

    logger.info('[Phase 3] No matching branch found')
    return None

def enhance_response_with_form_cta(
    response_text: str,
    user_message: str,
    tenant_hash: str,
    conversation_history: List[Dict[str, str]] = None,
    session_context: Dict[str, Any] = None  # PHASE 1B: NEW parameter
) -> Dict[str, Any]:
    """
    Enhance the HTTP response with form CTAs when appropriate
    This is the main entry point for HTTP mode enhancement

    Phase 1B Enhancement: Now supports session_context for parity with streaming mode
    - Tracks completed_forms to prevent duplicate CTAs
    - Loads conversation_branches and cta_definitions from config
    - Filters CTAs based on user's form completion history
    """

    try:
        # PHASE 1B: Extract completed forms and suspended forms from session context
        session_context = session_context or {}
        completed_forms = session_context.get('completed_forms', [])
        suspended_forms = session_context.get('suspended_forms', [])

        logger.info(f"[Phase 1B] Completed forms: {completed_forms}")
        logger.info(f"[Phase 1B] Suspended forms: {suspended_forms}")

        # PHASE 1B: If there are suspended forms, check if user is asking about a DIFFERENT program
        # This enables intelligent form switching UX
        if suspended_forms:
            logger.info(f"[Phase 1B] 🔄 Suspended form detected: {suspended_forms[0]}")

            # Load config to check if current message would trigger a DIFFERENT form
            config = load_tenant_config(tenant_hash)
            conversational_forms = config.get('conversational_forms', {})

            # Check if user's message would trigger a different form
            triggered_form = should_trigger_form(
                user_message,
                conversational_forms,
                readiness_score=0.8
            )

            if triggered_form:
                new_form_id = triggered_form.get('form_id')
                suspended_form_id = suspended_forms[0]

                # If user is asking about a DIFFERENT program, offer to switch
                if new_form_id != suspended_form_id:
                    logger.info(f"[Phase 1B] 🔀 Program switch detected! Suspended: {suspended_form_id}, Interested in: {new_form_id}")

                    # Get program names from form titles in config
                    new_program_name = triggered_form.get('title', 'this program').replace(' Application', '')

                    # Get suspended form's title - need to find the config by matching form_id
                    # The suspended_form_id might be the form_id value (e.g., "volunteer_apply")
                    # but the config key might be different (e.g., "volunteer_general")
                    suspended_form_config = None
                    for config_key, form_config in conversational_forms.items():
                        if form_config.get('form_id') == suspended_form_id or config_key == suspended_form_id:
                            suspended_form_config = form_config
                            break

                    if not suspended_form_config:
                        suspended_form_config = {}

                    suspended_program_name = suspended_form_config.get('title', 'your application').replace(' Application', '')

                    # If user selected a program_interest in the volunteer form, use that instead of "Volunteer"
                    program_interest = session_context.get('program_interest')
                    if program_interest:
                        program_map = {
                            'lovebox': 'Love Box',
                            'daretodream': 'Dare to Dream',
                            'both': 'both programs',
                            'unsure': 'Volunteer'
                        }
                        suspended_program_name = program_map.get(program_interest.lower(), suspended_program_name)
                        logger.info(f"[Phase 1B] 📝 User selected program_interest='{program_interest}', showing as '{suspended_program_name}'")

                    return {
                        "message": response_text,
                        "cards": [],  # No automatic CTAs - frontend will show switch options
                        "metadata": {
                            "enhanced": True,
                            "program_switch_detected": True,
                            "suspended_form": {
                                "form_id": suspended_form_id,
                                "program_name": suspended_program_name
                            },
                            "new_form_of_interest": {
                                "form_id": new_form_id,
                                "program_name": new_program_name,
                                "cta_text": triggered_form.get("cta_text", f"Apply to {new_program_name}"),
                                "fields": triggered_form.get("fields", [])
                            }
                        }
                    }

            # No different program detected - just skip CTAs as before
            logger.info(f"[Phase 1B] 🚫 Skipping form CTAs - suspended form active, no program switch detected")
            return {
                "message": response_text,
                "cards": [],  # No CTAs when form is suspended
                "metadata": {
                    "enhanced": False,
                    "suspended_forms_detected": suspended_forms
                }
            }

        # Load tenant configuration
        config = load_tenant_config(tenant_hash)
        conversational_forms = config.get('conversational_forms', {})
        conversation_branches = config.get('conversation_branches', {})
        cta_definitions = config.get('cta_definitions', {})

        # PHASE 3: Check for form triggers first (highest priority)
        triggered_form = should_trigger_form(
            user_message,
            conversational_forms,
            readiness_score=0.8  # Use default threshold
        )

        if triggered_form:
            # Map formId to program for comparison with completed_forms
            form_id = triggered_form.get('form_id')
            program = form_id  # Default to formId

            # Map specific formIds to programs
            if form_id == 'lb_apply':
                program = 'lovebox'
            elif form_id == 'dd_apply':
                program = 'daretodream'

            # Check if this program has already been completed
            if program in completed_forms:
                logger.info(f"[Phase 3] 🚫 Program '{program}' already completed (formId: {form_id}), skipping form trigger CTA")
                # Don't show this CTA - continue to branch detection
            else:
                logger.info(f"[Phase 3] ✅ Form trigger detected for program '{program}'")
                return {
                    "message": response_text,
                    "cards": [{
                        "type": "form_cta",
                        "label": triggered_form.get("cta_text", "Start Application"),
                        "action": "start_form",
                        "formId": form_id,
                        "fields": triggered_form.get("fields", [])
                    }],
                    "metadata": {
                        "enhanced": True,
                        "form_triggered": form_id,
                        "program": program
                    }
                }

        # PHASE 3: Detect conversation branch for general CTAs
        branch_result = detect_conversation_branch(
            response_text,
            user_message,
            config,
            completed_forms
        )

        logger.info(f"[Phase 3] Branch detection result: {branch_result}")

        if branch_result and branch_result.get('ctas'):
            # Convert CTAs to card format
            cards = []
            for cta in branch_result['ctas']:
                card = {
                    "type": cta.get("type", "cta_button"),
                    "label": cta.get("label") or cta.get("text"),
                    "action": cta.get("action", "link"),
                }

                # Add optional fields
                if cta.get("formId"):
                    card["formId"] = cta["formId"]
                if cta.get("url"):
                    card["url"] = cta["url"]
                if cta.get("fields"):
                    card["fields"] = cta["fields"]
                if cta.get("style"):
                    card["style"] = cta["style"]

                # PHASE 1B: Include program field for frontend filtering
                if cta.get("program"):
                    card["program"] = cta["program"]
                elif cta.get("program_id"):
                    card["program"] = cta["program_id"]

                cards.append(card)

            return {
                "message": response_text,
                "cards": cards,
                "metadata": {
                    "enhanced": True,
                    "branch_detected": branch_result.get("branch"),
                    "cta_count": len(cards)
                }
            }

        # No CTAs triggered
        logger.info("[Phase 3] No CTAs triggered (no form trigger or branch match)")
        return {
            "message": response_text,
            "cards": [],
            "metadata": {
                "enhanced": True,
                "no_ctas": True
            }
        }

    except Exception as e:
        logger.error(f"Error enhancing response: {e}")
        # On error, return unenhanced response
        return {
            "message": response_text,
            "cards": [],
            "metadata": {"error": str(e)}
        }

def extract_conversation_history(event: Dict[str, Any]) -> List[Dict[str, str]]:
    """Extract conversation history from the event"""

    try:
        body = json.loads(event.get('body', '{}')) if isinstance(event.get('body'), str) else event.get('body', {})

        # Check for conversation_context in body
        context = body.get('conversation_context', {})
        history = context.get('conversation_history', [])

        # Also check for messages in body directly
        if not history:
            history = body.get('messages', [])

        # Convert to standard format
        formatted_history = []
        for item in history:
            if isinstance(item, dict):
                if 'user' in item or 'assistant' in item:
                    formatted_history.append(item)
                elif 'content' in item and 'role' in item:
                    # Convert from role/content format
                    if item['role'] == 'user':
                        formatted_history.append({'user': item['content']})
                    elif item['role'] == 'assistant':
                        formatted_history.append({'assistant': item['content']})

        return formatted_history

    except Exception as e:
        logger.error(f"Error extracting conversation history: {e}")
        return []