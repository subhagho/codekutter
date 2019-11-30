package com.codekutter.common.model;

/**
 * Entity Interface for Entities that are part of defined
 * Tenants.
 */
public interface TenantEntity {
    /**
     * Get the tenant this Entity definition belongs to.
     *
     * @return - Owner Tenant.
     */
    Tenant getTenant();
}
