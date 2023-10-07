package hassmqtt

import (
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Device struct {
	Namespace string
	ClientID  string
	ID        string

	Name         string
	Manufacturer string
	Model        string
	HWVersion    string
	SWVersion    string
}

type Metric struct {
	Device      *Device
	ID          string
	DeviceType  string
	DeviceClass string
	Unit        string

	mut       sync.Mutex
	published time.Time
}

type hassConfig struct {
	DeviceClass       string     `json:"device_class"`
	StateTopic        string     `json:"state_topic"`
	UnitOfMeasurement string     `json:"unit_of_measurement,omitempty"`
	ValueTemplate     string     `json:"value_template,omitempty"`
	UniqueID          string     `json:"unique_id"`
	Device            hassDevice `json:"device"`
}

type hassDevice struct {
	Identifiers  []string `json:"identifiers"`
	Name         string   `json:"name"`
	Manufacturer string   `json:"manufacturer,omitempty"`
	Model        string   `json:"model,omitempty"`
	HWVersion    string   `json:"hw_version,omitempty"`
	SWVersion    string   `json:"sw_version,omitempty"`
}

func (m *Metric) topic() string {
	return path.Join(m.Device.Namespace, m.Device.ClientID, m.Device.ID, m.ID)
}

func (m *Metric) configTopic() string {
	return path.Join("homeassistant", m.DeviceType, strings.Join([]string{m.Device.Namespace, m.Device.ClientID, m.Device.ID, m.ID}, "-"), "config")
}

func (m *Metric) configPayload() *hassConfig {
	return &hassConfig{
		DeviceClass:       m.DeviceClass,
		StateTopic:        m.topic(),
		UnitOfMeasurement: m.Unit,
		UniqueID:          strings.Join([]string{m.Device.Namespace, m.Device.ClientID, m.Device.ID, m.ID}, "-"),
		Device: hassDevice{
			Identifiers:  []string{strings.Join([]string{m.Device.Namespace, m.Device.ClientID, m.Device.ID}, "-")},
			Name:         m.Device.Name,
			Manufacturer: m.Device.Manufacturer,
			Model:        m.Device.Model,
			HWVersion:    m.Device.HWVersion,
			SWVersion:    m.Device.SWVersion,
		},
	}
}

func (m *Metric) Publish(client mqtt.Client, value any) error {
	m.mut.Lock()
	defer m.mut.Unlock()

	if time.Since(m.published) > time.Minute {
		if err := sendMQTT(client, m.configTopic(), m.configPayload(), false); err != nil {
			return err
		}
		m.published = time.Now()
	}

	return sendMQTT(client, m.topic(), value, false)
}

func sendMQTT(client mqtt.Client, topic string, payload any, retain bool) error {
	bs, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	token := client.Publish(topic, 0, retain, bs)
	token.Wait()
	fmt.Printf("%v: %s: %s\n", token.Error(), topic, bs)
	return token.Error()
}
