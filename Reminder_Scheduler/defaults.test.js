'use strict';

/**
 * Unit tests for the real-AWS default factory (scheduler.buildDefaultDeps) — specifically
 * the idempotent CreateSchedule/DeleteSchedule wrappers (§E1 "deterministic, idempotent
 * rule names"): a re-create of an existing schedule and a delete of a missing one must both
 * be no-op successes, so commit/rebind/delete are safe to retry.
 */

const { mockClient } = require('aws-sdk-client-mock');
const {
  SchedulerClient,
  CreateScheduleCommand,
  DeleteScheduleCommand,
} = require('@aws-sdk/client-scheduler');
const scheduler = require('./scheduler');

const schedulerMock = mockClient(SchedulerClient);

beforeEach(() => schedulerMock.reset());

describe('buildDefaultDeps — config + shape', () => {
  test('reads config from env and exposes the injected-deps shape', () => {
    const deps = scheduler.buildDefaultDeps();
    expect(typeof deps.scheduler.createSchedule).toBe('function');
    expect(typeof deps.scheduler.deleteSchedule).toBe('function');
    expect(typeof deps.ddb.send).toBe('function');
    expect(typeof deps.now()).toBe('number');
    expect(deps.config.scheduledMessagesTable).toBeTruthy();
  });
});

describe('default scheduler wrapper — idempotency', () => {
  test('createSchedule succeeds normally', async () => {
    schedulerMock.on(CreateScheduleCommand).resolves({});
    const deps = scheduler.buildDefaultDeps();
    await expect(deps.scheduler.createSchedule({ Name: 'x' })).resolves.toBeUndefined();
  });

  test('createSchedule swallows ConflictException (already exists → idempotent)', async () => {
    const err = new Error('exists');
    err.name = 'ConflictException';
    schedulerMock.on(CreateScheduleCommand).rejects(err);
    const deps = scheduler.buildDefaultDeps();
    await expect(deps.scheduler.createSchedule({ Name: 'x' })).resolves.toBeUndefined();
  });

  test('createSchedule rethrows other errors', async () => {
    const err = new Error('boom');
    err.name = 'ValidationException';
    schedulerMock.on(CreateScheduleCommand).rejects(err);
    const deps = scheduler.buildDefaultDeps();
    await expect(deps.scheduler.createSchedule({ Name: 'x' })).rejects.toThrow('boom');
  });

  test('deleteSchedule swallows ResourceNotFoundException (already gone → idempotent)', async () => {
    const err = new Error('gone');
    err.name = 'ResourceNotFoundException';
    schedulerMock.on(DeleteScheduleCommand).rejects(err);
    const deps = scheduler.buildDefaultDeps();
    await expect(deps.scheduler.deleteSchedule({ Name: 'x', GroupName: 'g' })).resolves.toBeUndefined();
  });

  test('deleteSchedule rethrows other errors', async () => {
    const err = new Error('denied');
    err.name = 'AccessDeniedException';
    schedulerMock.on(DeleteScheduleCommand).rejects(err);
    const deps = scheduler.buildDefaultDeps();
    await expect(deps.scheduler.deleteSchedule({ Name: 'x', GroupName: 'g' })).rejects.toThrow('denied');
  });
});
