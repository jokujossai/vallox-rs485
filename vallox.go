// Package valloxrs485 implements Vallox RS485 protocol
package valloxrs485

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"time"

	"github.com/tarm/serial"
)

// Config foo
type Config struct {
	// Device file for rs485 device
	Device string
	// RemoteClientId is the id for this device in Vallox rs485 bus
	RemoteClientId byte
	// Enable writing to Vallox regisers, default false
	EnableWrite bool
	// Logge for debug, default no logging
	LogDebug *log.Logger
}

type Vallox struct {
	port           *serial.Port
	remoteClientId byte
	running        bool
	buffer         *bufio.ReadWriter
	in             chan Event
	out            chan valloxPackage
	lastActivity   time.Time
	writeAllowed   bool
	logDebug       *log.Logger
}

const (
	MsgDomain     = 0x01
	MsgPollByte   = 0x00
	MsgMainboard1 = 0x11
	MsgMainboards = 0x10
	MsgPanel1     = 0x21
	MsgPanels     = 0x20
)

// Registers based on Vallox documentation
const (
	RegisterIO07                 byte = 0x07
	RegisterIO08                 byte = 0x08
	RegisterCurrentFanSpeed      byte = 0x29
	RegisterMaxRH                byte = 0x2a
	RegisterCurrentCO2           byte = 0x2b
	RegisterMaximumCO2           byte = 0x2c
	RegisterCO2Status            byte = 0x2d
	RegisterMessage              byte = 0x2e
	RegisterRH1                  byte = 0x2f
	RegisterRH2                  byte = 0x30
	RegisterOutdoorTemp          byte = 0x32
	RegisterExhaustOutTemp       byte = 0x33
	RegisterExhaustInTemp        byte = 0x34
	RegisterSupplyTemp           byte = 0x35
	RegisterFaultCode            byte = 0x36
	RegisterPostHeatingOnTime    byte = 0x55
	RegisterPostHeatingOffTime   byte = 0x56
	RegisterPostHeatingTarget    byte = 0x57
	RegisterFlags02              byte = 0x6d
	RegisterFlags04              byte = 0x6f
	RegisterFlags05              byte = 0x70
	RegisterFlags06              byte = 0x71
	RegisterFireplaceCounter     byte = 0x79
	Register8f                   byte = 0x8f
	Register91                   byte = 0x91
	RegisterStatus               byte = 0xa3
	RegisterPostHeatingSetpoint  byte = 0xa4
	RegisterMaxFanSpeed          byte = 0xa5
	RegisterServiceInterval      byte = 0xa6
	RegisterPreheatingTemp       byte = 0xa7
	RegisterSupplyFanStopTemp    byte = 0xa8
	RegisterDefaultFanSpeed      byte = 0xa9
	RegisterProgram              byte = 0xaa
	RegisterServiceCounter       byte = 0xab
	RegisterBasicHumidity        byte = 0xae
	RegisterBypassTemp           byte = 0xaf
	RegisterSupplyFanSetpoint    byte = 0xb0
	RegisterExhaustFanSetpoint   byte = 0xb1
	RegisterAntiFreezeHysteresis byte = 0xb2
	RegisterCO2SetpointUpper     byte = 0xb3
	RegisterCO2SetpointLower     byte = 0xb4
	RegisterProgram2             byte = 0xb5
)

// Flags of variable 08
const (
	IO07FlagReheating byte = 0x20
)

// Flags of variable 08
const (
	IO08FlagSummerMode      byte = 0x02
	IO08FlagErrorRelay      byte = 0x04
	IO08FlagMotorIn         byte = 0x08
	IO08FlagPreheating      byte = 0x10
	IO08FlagMotorOut        byte = 0x20
	IO08FlagFireplaceSwitch byte = 0x40
)

// Fan speeds
const (
	FanSpeed1 byte = 0x01
	FanSpeed2 byte = 0x03
	FanSpeed3 byte = 0x07
	FanSpeed4 byte = 0x0f
	FanSpeed5 byte = 0x1f
	FanSpeed6 byte = 0x3f
	FanSpeed7 byte = 0x7f
	FanSpeed8 byte = 0xff
)

const RHOffset = 51
const RHDivider = 2.04

// Status flags of variable 2d
const (
	CO2Sensor1 byte = 0x02
	CO2Sensor2 byte = 0x04
	CO2Sensor3 byte = 0x08
	CO2Sensor4 byte = 0x10
	CO2Sensor5 byte = 0x20
)

// Status flags of variable 36
const (
	FaultSupplyAirSensorFault     byte = 0x05
	FaultCarbonDioxideAlarm       byte = 0x06
	FaultOutdoorSensorFault       byte = 0x07
	FaultExhaustAirInSensorFault  byte = 0x08
	FaultWaterCoilFreezing        byte = 0x09
	FaultExhaustAirOutSensorFault byte = 0x0a
)

const TimeDivider = 2.5

const (
	Flags2CO2HigherSpeedReq   byte = 0x01
	Flags2CO2LowerSpeedReq    byte = 0x02
	Flags2RHLowerSpeedReq     byte = 0x04
	Flags2SwitchLowerSpeedReq byte = 0x08
	Flags2CO2Alarm            byte = 0x40
	Flags2CellFreezeAlarm     byte = 0x80
)

const (
	Flags4WaterCoilFreezing byte = 0x10
	Flags4Master            byte = 0xf0
)

const (
	Flags5PreheatingStatus byte = 0xf0
)

const (
	Flags6RemoteControl           byte = 0x10
	Flags6ActivateFireplaceSwitch byte = 0x20
	Flags6FireplaceFunction       byte = 0x40
)

// Status flags of variable a3
const (
	StatusFlagPower       byte = 0x01
	StatusFlagCO2         byte = 0x02
	StatusFlagRH          byte = 0x04
	StatusFlagHeatingMode byte = 0x08
	StatusFlagFilter      byte = 0x10
	StatusFlagHeating     byte = 0x20
	StatusFlagFault       byte = 0x40
	StatusFlagService     byte = 0x80
)

const (
	ProgramFlagAutomaticHumidity byte = 0x10
	ProgramFlagBoostSwitch       byte = 0x20
	ProgramFlagWater             byte = 0x40
	ProgramFlagCascadeControl    byte = 0x80
)

const (
	Program2FlagMaximumSpeedLimit byte = 0x01
)

type Event struct {
	Time        time.Time   `json:"time"`
	Source      byte        `json:"source"`
	Destination byte        `json:"destination"`
	Register    byte        `json:"register"`
	RawValue    byte        `json:"raw"`
	Value       interface{} `json:"value"`
}

type valloxPackage struct {
	System      byte
	Source      byte
	Destination byte
	Register    byte
	Value       byte
	Checksum    byte
}

var writeAllowed = map[byte]bool{
	RegisterCurrentFanSpeed: true,
	RegisterMaxFanSpeed:     true,
	RegisterDefaultFanSpeed: true,
	RegisterProgram:         true,
}

// Open opens the rs485 device specified in Config
func Open(cfg Config) (*Vallox, error) {

	if cfg.LogDebug == nil {
		cfg.LogDebug = log.New(ioutil.Discard, "", 0)
	}

	if cfg.RemoteClientId == 0 {
		cfg.RemoteClientId = 0x27
	}

	if cfg.RemoteClientId < 0x20 || cfg.RemoteClientId > 0x2f {
		return nil, fmt.Errorf("invalid remoteClientId %x", cfg.RemoteClientId)
	}

	portCfg := &serial.Config{Name: cfg.Device, Baud: 9600, Size: 8, Parity: 'N', StopBits: 1}
	port, err := serial.OpenPort(portCfg)
	if err != nil {
		return nil, err
	}

	buffer := new(bytes.Buffer)
	vallox := &Vallox{
		port:           port,
		running:        true,
		buffer:         bufio.NewReadWriter(bufio.NewReader(buffer), bufio.NewWriter(buffer)),
		remoteClientId: cfg.RemoteClientId,
		// Queue size should be greater than count of sendInit messages
		in:           make(chan Event, 100),
		out:          make(chan valloxPackage, 100),
		writeAllowed: cfg.EnableWrite,
		logDebug:     cfg.LogDebug,
	}

	sendInit(vallox)

	go handleIncoming(vallox)
	go handleOutgoing(vallox)

	return vallox, nil
}

// Events returns channel for events from Vallox bus
func (vallox Vallox) Events() chan Event {
	return vallox.in
}

// ForMe returns true if event is addressed for this client
func (vallox Vallox) ForMe(e Event) bool {
	return e.Destination == MsgPanels || e.Destination == vallox.remoteClientId
}

// Query queries Vallox for register
func (vallox Vallox) Query(register byte) {
	pkg := createQuery(vallox, register)
	vallox.out <- *pkg
}

// SetSpeed changes speed of ventilation fan
func (vallox Vallox) SetSpeed(speed byte) {
	if speed < 1 || speed > 8 {
		vallox.logDebug.Printf("received invalid speed %x", speed)
		return
	}
	value := speedToValue(int8(speed))
	vallox.logDebug.Printf("received set speed %x", speed)
	// Send value to the main vallox device
	vallox.writeRegister(MsgMainboard1, RegisterCurrentFanSpeed, value)
	// Also publish value to all the remotes
	vallox.writeRegister(MsgPanels, RegisterCurrentFanSpeed, value)
}

// SetDefaultFanSpeed changes default speed of ventilation fan
func (vallox Vallox) SetDefaultFanSpeed(speed byte) {
	if speed < 1 || speed > 8 {
		vallox.logDebug.Printf("received invalid speed %x", speed)
		return
	}
	value := speedToValue(int8(speed))
	vallox.logDebug.Printf("received set speed %x", speed)
	// Send value to the main vallox device
	vallox.writeRegister(MsgMainboard1, RegisterDefaultFanSpeed, value)
	// Also publish value to all the remotes
	vallox.writeRegister(MsgPanels, RegisterDefaultFanSpeed, value)
}

// SetMaxFanSpeed changes maximum speed of ventilation fan
func (vallox Vallox) SetMaxFanSpeed(speed byte) {
	if speed < 1 || speed > 8 {
		vallox.logDebug.Printf("received invalid speed %x", speed)
		return
	}
	value := speedToValue(int8(speed))
	vallox.logDebug.Printf("received set speed %x", speed)
	// Send value to the main vallox device
	vallox.writeRegister(MsgMainboard1, RegisterMaxFanSpeed, value)
	// Also publish value to all the remotes
	vallox.writeRegister(MsgPanels, RegisterMaxFanSpeed, value)
}

// Query all known registers
func sendInit(vallox *Vallox) {
	vallox.Query(RegisterIO07)
	vallox.Query(RegisterIO08)
	vallox.Query(RegisterCurrentFanSpeed)
	vallox.Query(RegisterMaxRH)
	vallox.Query(RegisterCurrentCO2)
	vallox.Query(RegisterMaximumCO2)
	vallox.Query(RegisterCO2Status)
	vallox.Query(RegisterMessage)
	vallox.Query(RegisterRH1)
	vallox.Query(RegisterRH2)
	vallox.Query(RegisterOutdoorTemp)
	vallox.Query(RegisterExhaustOutTemp)
	vallox.Query(RegisterExhaustInTemp)
	vallox.Query(RegisterSupplyTemp)
	vallox.Query(RegisterFaultCode)
	vallox.Query(RegisterPostHeatingOnTime)
	vallox.Query(RegisterPostHeatingOffTime)
	vallox.Query(RegisterPostHeatingTarget)
	vallox.Query(RegisterFlags02)
	vallox.Query(RegisterFlags04)
	vallox.Query(RegisterFlags05)
	vallox.Query(RegisterFlags06)
	vallox.Query(RegisterFireplaceCounter)
	vallox.Query(RegisterStatus)
	vallox.Query(RegisterPostHeatingSetpoint)
	vallox.Query(RegisterMaxFanSpeed)
	vallox.Query(RegisterServiceInterval)
	vallox.Query(RegisterPreheatingTemp)
	vallox.Query(RegisterSupplyFanStopTemp)
	vallox.Query(RegisterDefaultFanSpeed)
	vallox.Query(RegisterProgram)
	vallox.Query(RegisterServiceCounter)
	vallox.Query(RegisterBasicHumidity)
	vallox.Query(RegisterBypassTemp)
	vallox.Query(RegisterSupplyFanSetpoint)
	vallox.Query(RegisterExhaustFanSetpoint)
	vallox.Query(RegisterAntiFreezeHysteresis)
	vallox.Query(RegisterCO2SetpointUpper)
	vallox.Query(RegisterCO2SetpointLower)
	vallox.Query(RegisterProgram2)
}

func (vallox Vallox) writeRegister(destination byte, register byte, value byte) {
	pkg := createWrite(vallox, destination, register, value)
	vallox.out <- *pkg
}

func createQuery(vallox Vallox, register byte) *valloxPackage {
	return createWrite(vallox, MsgMainboard1, 0, register)
}

func createWrite(vallox Vallox, destination byte, register byte, value byte) *valloxPackage {
	pkg := new(valloxPackage)
	pkg.System = 1
	pkg.Source = vallox.remoteClientId
	pkg.Destination = destination
	pkg.Register = register
	pkg.Value = value
	pkg.Checksum = calculateChecksum(pkg)
	return pkg
}

func handleOutgoing(vallox *Vallox) {
	for vallox.running {
		pkg := <-vallox.out

		if !isOutgoingAllowed(vallox, pkg.Register) {
			vallox.logDebug.Printf("outgoing not allowed for %x = %x", pkg.Register, pkg.Value)
			continue
		}

		now := time.Now()
		if vallox.lastActivity.IsZero() || now.UnixMilli()-vallox.lastActivity.UnixMilli() < 50 {
			vallox.logDebug.Printf("delay outgoing to %x %x = %x, lastActivity %v now %v, diff %d ms",
				pkg.Destination, pkg.Register, pkg.Value, vallox.lastActivity, now, now.UnixMilli()-vallox.lastActivity.UnixMilli())
			time.Sleep(time.Millisecond * 50)
		}
		updateLastActivity(vallox)
		binary.Write(vallox.port, binary.BigEndian, pkg)
	}
}

func isOutgoingAllowed(vallox *Vallox, register byte) bool {
	if register == 0 {
		// queries are allowed
		return true
	}

	if !vallox.writeAllowed {
		return false
	}

	return writeAllowed[register]
}

func handleIncoming(vallox *Vallox) {
	vallox.running = true
	buf := make([]byte, 6)
	for vallox.running {
		n, err := vallox.port.Read(buf)
		if err != nil {
			fatalError(err, vallox)
			return
		}
		if n > 0 {
			updateLastActivity(vallox)
			vallox.buffer.Write(buf[:n])
			vallox.buffer.Writer.Flush()
			handleBuffer(vallox)
		}
	}
}

func updateLastActivity(vallox *Vallox) {
	vallox.lastActivity = time.Now()
}

func fatalError(err error, vallox *Vallox) {
	vallox.running = false
}

func handleBuffer(vallox *Vallox) {
	for {
		buf, err := vallox.buffer.Peek(6)
		if err != nil && err == io.EOF {
			// not enough bytes, ok, continue
			return
		} else if err != nil {
			fatalError(err, vallox)
			return
		}
		pkg := validPackage(buf)
		if pkg != nil {
			vallox.buffer.Discard(6)
			handlePackage(pkg, vallox)
		} else {
			// discard byte, since no valid package starts here
			vallox.buffer.ReadByte()
		}
	}
}

func handlePackage(pkg *valloxPackage, vallox *Vallox) {
	vallox.in <- *event(pkg, vallox)
}

func event(pkg *valloxPackage, vallox *Vallox) *Event {
	event := new(Event)
	event.Time = time.Now()
	event.Source = pkg.Source
	event.Destination = pkg.Destination
	event.Register = pkg.Register
	event.RawValue = pkg.Value
	switch pkg.Register {
	// Speed conversion
	case RegisterCurrentFanSpeed:
		fallthrough
	case RegisterMaxFanSpeed:
		fallthrough
	case RegisterDefaultFanSpeed:
		event.Value = int16(valueToSpeed(pkg.Value))
	// RH conversion
	case RegisterMaxRH:
		fallthrough
	case RegisterRH1:
		fallthrough
	case RegisterRH2:
		fallthrough
	case RegisterBasicHumidity:
		event.Value = math.Round(float64(valueToRh(pkg.Value))*100) / 100
	// Temperature conversion
	case RegisterOutdoorTemp:
		fallthrough
	case RegisterExhaustOutTemp:
		fallthrough
	case RegisterExhaustInTemp:
		fallthrough
	case RegisterSupplyTemp:
		fallthrough
	case RegisterPostHeatingTarget:
		fallthrough
	case RegisterPostHeatingSetpoint:
		fallthrough
	case RegisterPreheatingTemp:
		fallthrough
	case RegisterBypassTemp:
		event.Value = int16(valueToTemp(pkg.Value))
	// Percentage conversion
	case RegisterPostHeatingOnTime:
		fallthrough
	case RegisterPostHeatingOffTime:
		event.Value = float64(pkg.Value) / 2.5
	// Plain value
	default:
		event.Value = int16(pkg.Value)
	}
	return event
}

func valueToSpeed(value byte) int8 {
	for i, v := range fanSpeedConversion {
		if value == v {
			return int8(i) + 1
		}
	}
	return -1
}

func speedToValue(speed int8) byte {
	return fanSpeedConversion[speed-1]
}

func valueToRh(value byte) float64 {
	return (float64(value) + RHOffset) / RHDivider
}

func valueToTemp(value byte) int8 {
	return tempConversion[value]
}

func validPackage(buffer []byte) (pkg *valloxPackage) {
	pkg = new(valloxPackage)
	err := binary.Read(bytes.NewReader(buffer), binary.LittleEndian, pkg)

	if err == nil && validChecksum(pkg) {
		return pkg
	}

	return nil
}

func validChecksum(pkg *valloxPackage) bool {
	return pkg.Checksum == calculateChecksum(pkg)
}

func calculateChecksum(pkg *valloxPackage) byte {
	return pkg.System + pkg.Source + pkg.Destination + pkg.Register + pkg.Value
}

var fanSpeedConversion = [8]byte{
	FanSpeed1,
	FanSpeed2,
	FanSpeed3,
	FanSpeed4,
	FanSpeed5,
	FanSpeed6,
	FanSpeed7,
	FanSpeed8,
}

var tempConversion = [256]int8{
	-74, -70, -66, -62, -59, -56, -54, -52, -50, -48, -47, -46, -44, -43, -42, -41,
	-40, -39, -38, -37, -36, -35, -34, -33, -33, -32, -31, -30, -30, -29, -28, -28, -27, -27, -26, -25, -25,
	-24, -24, -23, -23, -22, -22, -21, -21, -20, -20, -19, -19, -19, -18, -18, -17, -17, -16, -16, -16, -15,
	-15, -14, -14, -14, -13, -13, -12, -12, -12, -11, -11, -11, -10, -10, -9, -9, -9, -8, -8, -8, -7, -7, -7,
	-6, -6, -6, -5, -5, -5, -4, -4, -4, -3, -3, -3, -2, -2, -2, -1, -1, -1, -1, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3,
	3, 4, 4, 4, 5, 5, 5, 5, 6, 6, 6, 7, 7, 7, 8, 8, 8, 9, 9, 9, 10, 10, 10, 11, 11, 11, 12, 12, 12, 13, 13, 13,
	14, 14, 14, 15, 15, 15, 16, 16, 16, 17, 17, 18, 18, 18, 19, 19, 19, 20, 20, 21, 21, 21, 22, 22, 22, 23, 23,
	24, 24, 24, 25, 25, 26, 26, 27, 27, 27, 28, 28, 29, 29, 30, 30, 31, 31, 32, 32, 33, 33, 34, 34, 35, 35, 36,
	36, 37, 37, 38, 38, 39, 40, 40, 41, 41, 42, 43, 43, 44, 45, 45, 46, 47, 48, 48, 49, 50, 51, 52, 53, 53, 54,
	55, 56, 57, 59, 60, 61, 62, 63, 65, 66, 68, 69, 71, 73, 75, 77, 79, 81, 82, 86, 90, 93, 97, 100, 100, 100,
	100, 100, 100, 100, 100, 100,
}
