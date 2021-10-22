package kafka

import (
	"context"
	"fmt"

	"github.com/ONSdigital/log.go/v2/log"
	"github.com/Shopify/sarama"
)

type TopicAuth struct {
	App        string
	Subnets    []string
	Topic      string
	Operations []sarama.AclOperation
	Hosts      []string
}

type TopicAuthList struct {
	Domain  string
	Brokers []string
	Acls    []TopicAuth
}

type Acls []*sarama.AclCreation

// NewAdmin creates an admin-based client
func NewAdmin(brokerAddrs []string, adminCfg *AdminConfig) (sarama.ClusterAdmin, error) {
	config, err := adminCfg.Get()
	if err != nil {
		return nil, fmt.Errorf("failed to get admin config: %w", err)
	}
	return sarama.NewClusterAdmin(brokerAddrs, config)
}

func (t Acls) Apply(adm sarama.ClusterAdmin) error {
	for _, acl := range t {
		log.Info(context.Background(), "creating ACL", log.Data{"res": acl.Resource, "acl": acl.Acl})
		if err := adm.CreateACL(acl.Resource, acl.Acl); err != nil {
			return fmt.Errorf("error creating ACL : %w", err)
		}
		log.Info(context.Background(), "created ACL")
	}
	return nil
}

func (t TopicAuthList) Apply(adm sarama.ClusterAdmin) error {
	for _, topicAcl := range t.Acls {
		acls := topicAcl.GetAcls(t.Domain)
		if err := acls.Apply(adm); err != nil {
			return fmt.Errorf("error applying cluster-admin ACLs: %w", err)
		}

	}
	return nil
}

func (t TopicAuth) GetAcls(domain string) Acls {
	acls := make([]*sarama.AclCreation, 0)
	for _, subnet := range t.Subnets {
		for _, op := range t.Operations {
			for _, host := range t.Hosts {
				aclCreation := &sarama.AclCreation{
					Resource: sarama.Resource{
						ResourceType:        sarama.AclResourceTopic,
						ResourceName:        t.Topic,
						ResourcePatternType: sarama.AclPatternLiteral,
					},
					Acl: sarama.Acl{
						Principal:      GetPrincipal(t.App, subnet, domain),
						Host:           host,
						Operation:      op,
						PermissionType: sarama.AclPermissionAllow,
					},
				}
				acls = append(acls, aclCreation)
			}
		}
	}
	return acls
}

func GetPrincipal(app, subnet, domain string) string {
	return "User:CN=" + app + "." + subnet + "." + domain
}
