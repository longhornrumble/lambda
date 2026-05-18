"""
Unit tests for sub-phase A8 — scheduling fields on the employee registry.

Covers tenant_registry_ops.validate_scheduling_tags (pure) and the
scheduling_tags / calendar_email_override write path in put_employee:
presence, forward-compatible omission, reject-before-write on an
out-of-vocabulary tag, the B5 (vocabulary=None) path, and that an
old-shape item still unmarshalls cleanly.
"""

import pytest
from unittest.mock import patch, MagicMock

import tenant_registry_ops


class TestValidateSchedulingTags:
    def test_subset_passes(self):
        # No raise when every tag is in the vocabulary.
        tenant_registry_ops.validate_scheduling_tags(['a', 'b'], ['a', 'b', 'c'])

    def test_out_of_vocab_raises_listing_offenders(self):
        with pytest.raises(ValueError, match=r"\['x'\]"):
            tenant_registry_ops.validate_scheduling_tags(['a', 'x'], ['a', 'b'])

    def test_empty_or_missing_tags_is_noop(self):
        tenant_registry_ops.validate_scheduling_tags([], ['a'])
        tenant_registry_ops.validate_scheduling_tags(None, ['a'])

    def test_vocabulary_none_skips_validation(self):
        # B5 path: caller has not resolved the vocabulary yet — do not raise.
        tenant_registry_ops.validate_scheduling_tags(['anything'], None)


class TestPutEmployeeSchedulingFields:
    def _record(self, **extra):
        rec = {'email': 'a@b.co', 'name': 'A', 'role': 'member', 'status': 'active'}
        rec.update(extra)
        return rec

    def test_writes_scheduling_fields_when_present(self):
        with patch.object(tenant_registry_ops, 'dynamodb', MagicMock()) as ddb:
            tenant_registry_ops.put_employee(
                'MYR1', 'emp1',
                self._record(scheduling_tags=['weekend', 'spanish'],
                             calendar_email_override='cal@b.co'),
                scheduling_tag_vocabulary=['weekend', 'spanish', 'mentor'],
            )
        item = ddb.put_item.call_args.kwargs['Item']
        assert item['scheduling_tags'] == {'L': [{'S': 'weekend'}, {'S': 'spanish'}]}
        assert item['calendar_email_override'] == {'S': 'cal@b.co'}

    def test_omits_fields_when_absent(self):
        # Forward-compat: a record without the new fields writes no such keys.
        with patch.object(tenant_registry_ops, 'dynamodb', MagicMock()) as ddb:
            tenant_registry_ops.put_employee('MYR1', 'emp1', self._record())
        item = ddb.put_item.call_args.kwargs['Item']
        assert 'scheduling_tags' not in item
        assert 'calendar_email_override' not in item

    def test_invalid_tag_rejected_before_write(self):
        with patch.object(tenant_registry_ops, 'dynamodb', MagicMock()) as ddb:
            with pytest.raises(ValueError):
                tenant_registry_ops.put_employee(
                    'MYR1', 'emp1',
                    self._record(scheduling_tags=['ghost']),
                    scheduling_tag_vocabulary=['weekend'],
                )
            ddb.put_item.assert_not_called()

    def test_vocabulary_none_persists_without_validation(self):
        # B5 path: tags persist even though the vocabulary is unresolved.
        with patch.object(tenant_registry_ops, 'dynamodb', MagicMock()) as ddb:
            tenant_registry_ops.put_employee(
                'MYR1', 'emp1', self._record(scheduling_tags=['unchecked']))
        item = ddb.put_item.call_args.kwargs['Item']
        assert item['scheduling_tags'] == {'L': [{'S': 'unchecked'}]}


class TestUnmarshallForwardCompat:
    def test_old_shape_item_unmarshalls_without_new_keys(self):
        old = {
            'tenantId': {'S': 'MYR1'}, 'employeeId': {'S': 'e1'},
            'email': {'S': 'a@b.co'}, 'name': {'S': 'A'},
        }
        result = tenant_registry_ops._unmarshall(old)
        assert 'scheduling_tags' not in result
        assert 'calendar_email_override' not in result
        assert result['email'] == 'a@b.co'
