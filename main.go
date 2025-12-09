package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"

	pgxuuid "github.com/jackc/pgx-gofrs-uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tryy3/cozytemp-calibration-service/config"
	"github.com/tryy3/cozytemp-service-helper/kafka"
	"github.com/tryy3/cozytemp-service-helper/models"
)

func main() {
	slog.SetDefault(
		slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	)
	cfg := config.Load()

	slog.Info("Starting CozyTemp Calibration Service")
	slog.Info("Kafka Brokers", "brokers", cfg.KafkaBrokers)
	slog.Info("Consumer Topic", "topic", cfg.KafkaConsumerTopic)
	slog.Info("Producer Topic", "topic", cfg.KafkaProducerTopic)
	slog.Info("Consumer Group", "group", cfg.KafkaConsumerGroup)
	consumer := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaConsumerTopic, cfg.KafkaConsumerGroup)
	defer consumer.Close()

	producer := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaProducerTopic)
	defer producer.Close()

	for {
		msg, err := consumer.FetchMessage(context.Background())
		if err != nil {
			slog.Error("Error reading message", "error", err)
			continue
		}

		var calibrationData models.BeforeCalibrationDataEvent
		err = json.Unmarshal(msg.Value, &calibrationData)
		if err != nil {
			slog.Error("Error unmarshalling message", "error", err)
			continue
		}
		slog.Info("Calibration Data", "data", calibrationData)

		calibratedTemperature, err := calibrateData(&calibrationData)
		if err != nil {
			slog.Error("Error calibrating data", "error", err)
			continue
		}
		slog.Info("Data calibrated successfully")

		afterCalibrationData := models.AfterCalibrationDataEvent{
			NodeIdentifier:        calibrationData.NodeIdentifier,
			NodeID:                calibrationData.NodeID,
			SensorIdentifier:      calibrationData.SensorIdentifier,
			SensorID:              calibrationData.SensorID,
			RawDataID:             calibrationData.RawDataID,
			RawTemperature:        calibrationData.RawTemperature,
			CalibratedTemperature: calibratedTemperature,
		}
		slog.Info("After Calibration Data", "data", afterCalibrationData)

		skipCompletedMessage := cfg.KafkaProducerTopic == ""
		err = insertCalibratedData(cfg, &afterCalibrationData)
		if err != nil && strings.Contains(err.Error(), "duplicate key value violates unique constraint") {
			slog.Info("Calibrated data already exists, skipping insertion")
			skipCompletedMessage = true
		} else if err != nil {
			slog.Error("Error inserting calibrated data", "error", err)
			continue
		} else {
			slog.Info("Calibrated data inserted successfully")
		}

		if !skipCompletedMessage {
			jsonData, err := json.Marshal(afterCalibrationData)
			if err != nil {
				slog.Error("Error marshalling after calibration data", "error", err)
				continue
			}
			err = producer.WriteMessage(context.Background(), []byte(calibrationData.NodeIdentifier), jsonData)
			if err != nil {
				slog.Error("Error producing message", "error", err)
				continue
			}
			slog.Info("Message produced successfully")
		} else {
			slog.Info("Node identifier is empty, skipping message production")
		}

		err = consumer.CommitMessages(context.Background(), msg)
		if err != nil {
			slog.Error("Error committing message", "error", err)
			continue
		}
		slog.Info("Message committed successfully")
	}
}

// TODO: For now hard code this values, but make it more dynamic in the future
var (
	sensorValues  = []float64{21.74, 10.64, 20.9733, 10.7386, 21.1756, 10.9283}
	controlValues = []float64{22.5, 12.2, 22.1, 12.2, 21.6, 12.4}
)

// Linear regression formula: y = ax + b
// A_values is the sensor values
// B_values is the control values
// Returns the slope (a) and intercept (b)
func linearRegression(A_values []float64, B_values []float64) (float64, float64) {
	n := len(A_values)
	var sumA, sumB, sumAB, sumASquared float64

	for i := 0; i < n; i++ {
		sumA += A_values[i]
		sumB += B_values[i]
		sumAB += A_values[i] * B_values[i]
		sumASquared += A_values[i] * A_values[i]
	}

	resultA := (float64(n)*sumAB - sumA*sumB) / (float64(n)*sumASquared - sumA*sumA)
	resultB := (sumB - resultA*sumA) / float64(n)

	return resultA, resultB
}

func calibrateData(data *models.BeforeCalibrationDataEvent) (float64, error) {
	resultA, resultB := linearRegression(sensorValues, controlValues)

	calibratedTemperature := (resultA * data.RawTemperature) + resultB
	slog.Info("Calibrated Temperature", "temperature", calibratedTemperature, "resultA", resultA, "resultB", resultB, "rawTemperature", data.RawTemperature)

	return calibratedTemperature, nil
}

func connectToPostgres(cfg *config.Config) (*pgxpool.Pool, error) {
	dbconfig, err := pgxpool.ParseConfig(cfg.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("error parsing Postgres URL: %w", err)
	}
	dbconfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		pgxuuid.Register(conn.TypeMap())
		return nil
	}

	db, err := pgxpool.NewWithConfig(context.Background(), dbconfig)
	if err != nil {
		return nil, fmt.Errorf("error connecting to Postgres: %w", err)
	}
	if err := db.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("error pinging Postgres: %w", err)
	}
	return db, nil
}

func insertCalibratedData(cfg *config.Config, data *models.AfterCalibrationDataEvent) error {
	db, err := connectToPostgres(cfg)
	if err != nil {
		slog.Error("Error connecting to Postgres", "error", err)
		return fmt.Errorf("error connecting to Postgres: %w", err)
	}
	defer db.Close()

	tx, err := db.Begin(context.Background())
	if err != nil {
		slog.Error("Error beginning transaction", "error", err)
		return fmt.Errorf("error beginning transaction: %w", err)
	}
	defer tx.Rollback(context.Background())

	_, err = tx.Exec(context.Background(), "INSERT INTO calibrated_temperature (\"rawDataId\", \"temperature\") VALUES ($1, $2)", data.RawDataID, data.CalibratedTemperature)
	if err != nil {
		slog.Error("Error inserting calibrated data", "error", err)
		return fmt.Errorf("error inserting calibrated data: %w", err)
	}
	err = tx.Commit(context.Background())
	if err != nil {
		slog.Error("Error committing transaction", "error", err)
		return fmt.Errorf("error committing transaction: %w", err)
	}

	return nil
}
