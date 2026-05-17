// DeleteByTenant removes all logs for a tenant (admin-only, called via API Gateway RBAC).
package handler

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

// DeleteByTenant drops all logs belonging to the authenticated tenant.
func (h *SearchHandler) DeleteByTenant(c *gin.Context) {
	tenantID := c.GetHeader("X-Tenant-ID")
	if tenantID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"code": "BAD_REQUEST", "message": "X-Tenant-ID required"})
		return
	}
	query := fmt.Sprintf("ALTER TABLE logflow.logs_local DELETE WHERE tenant_id = '%s'", tenantID)
	if err := h.ch.Exec(c.Request.Context(), query); err != nil {
		h.log.Error("delete failed", zap.String("tenant", tenantID), zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"code": "DELETE_ERROR", "message": err.Error()})
		return
	}
	// Invalidate all cached searches for this tenant.
	pattern := "search:" + tenantID + ":*"
	iter := h.redis.Scan(c.Request.Context(), 0, pattern, 0).Iterator()
	for iter.Next(c.Request.Context()) {
		h.redis.Del(c.Request.Context(), iter.Val())
	}
	c.JSON(http.StatusOK, gin.H{"message": "logs scheduled for deletion", "tenant_id": tenantID})
}

// Suppress unused import — zap is used in SearchHandler methods.
var _ = fmt.Sprintf
