package hassmqtt

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
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
	Name        string
	StateClass  string

	mut       sync.Mutex
	published time.Time
}

type hassConfig struct {
	Name              string     `json:"name,omitempty"`
	DeviceClass       string     `json:"device_class"`
	StateTopic        string     `json:"state_topic"`
	UnitOfMeasurement string     `json:"unit_of_measurement,omitempty"`
	ValueTemplate     string     `json:"value_template,omitempty"`
	UniqueID          string     `json:"unique_id"`
	Device            hassDevice `json:"device"`
	StateClass        string     `json:"state_class,omitempty"`
}

type hassDevice struct {
	Identifiers  []string `json:"identifiers"`
	Name         string   `json:"name"`
	Manufacturer string   `json:"manufacturer,omitempty"`
	Model        string   `json:"model,omitempty"`
	HWVersion    string   `json:"hw_version,omitempty"`
	SWVersion    string   `json:"sw_version,omitempty"`
}

func (m *Metric) Topic() string {
	return path.Join(m.Device.Namespace, m.Device.ClientID, m.Device.ID, m.ID)
}

func (m *Metric) configTopic() string {
	return path.Join("homeassistant", m.DeviceType, strings.Join([]string{m.Device.Namespace, m.Device.ClientID, m.Device.ID, m.ID}, "-"), "config")
}

func (m *Metric) configPayload() *hassConfig {
	return &hassConfig{
		Name:              m.Name,
		DeviceClass:       m.DeviceClass,
		StateTopic:        m.Topic(),
		UnitOfMeasurement: m.Unit,
		UniqueID:          strings.Join([]string{m.Device.Namespace, m.Device.ID, m.ID}, "-"),
		StateClass:        m.StateClass,

		Device: hassDevice{
			Identifiers:  []string{strings.Join([]string{m.Device.Namespace, m.Device.ID}, "-")},
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

	return sendMQTT(client, m.Topic(), value, false)
}

func sendMQTT(client mqtt.Client, topic string, payload any, retain bool) error {
	bs, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	token := client.Publish(topic, 0, retain, bs)
	token.Wait()
	return token.Error()
}

func ClientID(prefix string) string {
	hn, _ := os.Hostname()
	home, _ := os.UserHomeDir()
	hf := sha256.New()
	fmt.Fprintf(hf, "%s\n%s\n%s\n", prefix, hn, home)
	hash := fmt.Sprintf("h%x", hf.Sum(nil))[:8]
	if hn == "" {
		hn = "unknown"
	}
	shortHost, _, _ := strings.Cut(hn, ".")
	return fmt.Sprintf("%s-%s-%s", prefix, shortHost, hash)
}
