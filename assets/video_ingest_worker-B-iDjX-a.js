/**
 * =============================================================================
 * video_ingest_worker.js
 * ORIGEN: Necesidad de estandarizar feeds de video masivos (MOV, MKV, MP4 RAW).
 * RESOLUCIÓN: Pipeline completo VideoDecoder -> OffscreenCanvas (Resize) -> VideoEncoder.
 * AXIOMA: Transcodificación Hardware-Accelerated a H.264 Canónico.
 * CUMPLIMIENTO TGS: Preparación del IdentityMap para Geometría Temporal.
 * LEY DE ADUANA (ADR-008): Solo materia canónica cruza la frontera al Core.
 * =============================================================================
 */

import * as Mp4BoxPkg from 'mp4box';
import * as Mp4Muxer from 'mp4-muxer';

const MP4Box = Mp4BoxPkg.default || Mp4BoxPkg;

self.onmessage = async (e) => {
    const { type, file, config, id } = e.data;

    if (type === 'INGEST') {
        try {
            console.log(`[MIE VideoWorker] Iniciando Ingesta: ${file.name}`);

            // 1. Preparar Encoders (Se instanciarán tras el onReady)
            let muxer;
            let videoEncoder;
            let audioEncoder;
            let videoConfigured = false;

            const setupEncoders = (vTrack, aTrack) => {
                // Cálculo de dimensiones canónicas (según preset)
                let width = vTrack.video.width;
                let height = vTrack.video.height;
                const maxRes = config.video.max_resolution || 1080;

                if (height > maxRes) {
                    width = Math.round((width * maxRes) / height);
                    height = maxRes;
                }
                
                // H.264 requiere dimensiones pares
                width = width % 2 === 0 ? width : width + 1;
                height = height % 2 === 0 ? height : height + 1;

                // INSTANCIACIÓN DETERMINISTA DEL MUXER (Elimina Deuda 6)
                muxer = new Mp4Muxer.Muxer({
                    target: new Mp4Muxer.ArrayBufferTarget(),
                    video: {
                        codec: 'avc',
                        width: width,
                        height: height
                    },
                    fastStart: 'in-memory' 
                });

                videoEncoder = new VideoEncoder({
                    output: (chunk, metadata) => muxer.addVideoChunk(chunk, metadata),
                    error: (e) => console.error("[MIE VideoEncoder] Error:", e)
                });

                videoEncoder.configure({
                    codec: config.video.codec || 'avc1.4d002a',
                    width: width,
                    height: height,
                    bitrate: config.video.target_bitrate || 5_000_000,
                    framerate: config.video.fps_cap || 30,
                    hardwareAcceleration: config.video.hardware_accel || 'prefer-hardware',
                    avc: { format: 'avc' }
                });

                if (aTrack) {
                    audioEncoder = new AudioEncoder({
                        output: (chunk, metadata) => muxer.addAudioChunk(chunk, metadata),
                        error: (e) => console.error("[MIE AudioEncoder] Error:", e)
                    });
                    audioEncoder.configure({
                        codec: config.audio.codec || 'mp4a.40.2',
                        numberOfChannels: aTrack.audio.channel_count,
                        sampleRate: aTrack.audio.sample_rate,
                        bitrate: config.audio.bitrate || 128_000
                    });
                }

                videoConfigured = true;
                return { width, height };
            };

            // 3. Demuxing (MP4Box)
            const mp4boxfile = MP4Box.createFile();
            let videoDecoder;
            let audioDecoder;
            let canvas;
            let ctx;

            mp4boxfile.onReady = (info) => {
                const vTrack = info.videoTracks[0];
                const aTrack = info.audioTracks[0];
                if (!vTrack) throw new Error("No video track found");

                const { width, height } = setupEncoders(vTrack, aTrack);

                // Preparar Canvas para Resizing (Axioma: Transcodificación Espacial)
                canvas = new OffscreenCanvas(width, height);
                ctx = canvas.getContext('2d');

                videoDecoder = new VideoDecoder({
                    output: (frame) => {
                        // Resizing si es necesario
                        ctx.drawImage(frame, 0, 0, width, height);
                        const newFrame = new VideoFrame(canvas, { timestamp: frame.timestamp });
                        videoEncoder.encode(newFrame);
                        newFrame.close();
                        frame.close();
                    },
                    error: (e) => console.error("[MIE VideoDecoder] Error:", e)
                });

                // Extraer avcC para configurar el decoder si es H264
                // Simplificado para la fase 1; en prod usar extracción de stsd.avcC
                videoDecoder.configure({
                    codec: vTrack.codec,
                    width: vTrack.video.width,
                    height: vTrack.video.height
                });

                if (aTrack && audioEncoder) {
                    audioDecoder = new AudioDecoder({
                        output: (data) => {
                            audioEncoder.encode(data);
                            data.close();
                        },
                        error: (e) => console.error("[MIE AudioDecoder] Error:", e)
                    });
                    audioDecoder.configure({
                        codec: aTrack.codec,
                        numberOfChannels: aTrack.audio.channel_count,
                        sampleRate: aTrack.audio.sample_rate
                    });
                }

                mp4boxfile.setExtractionOptions(vTrack.id);
                if (aTrack) mp4boxfile.setExtractionOptions(aTrack.id);
                mp4boxfile.start();
            };

            mp4boxfile.onSamples = (trackId, user, samples) => {
                for (const sample of samples) {
                    const timestamp = (sample.cts / sample.timescale) * 1_000_000;
                    const duration = (sample.duration / sample.timescale) * 1_000_000;

                    // Detectar si es video o audio basado en el track ID (simplificado por brevedad)
                    // En Mp4Box trackId indica cuál es cuál
                    const isVideo = trackId === mp4boxfile.getInfo().videoTracks[0].id;

                    if (isVideo && videoDecoder) {
                        videoDecoder.decode(new EncodedVideoChunk({
                            type: sample.is_sync ? 'key' : 'delta',
                            timestamp,
                            duration,
                            data: sample.data
                        }));
                    } else if (!isVideo && audioDecoder) {
                        audioDecoder.decode(new EncodedAudioChunk({
                            type: 'key',
                            timestamp,
                            duration,
                            data: sample.data
                        }));
                    }
                }
            };

            // Lectura por bloques (Axioma de Memoria Eficiente)
            const buffer = await file.arrayBuffer();
            buffer.fileStart = 0;
            mp4boxfile.appendBuffer(buffer);
            mp4boxfile.flush();

            // 4. FINALIZACIÓN DETERMINISTA
            // Esperamos a que todos los buffers se vacíen en el muxer
            if (videoDecoder) await videoDecoder.flush();
            if (audioDecoder) await audioDecoder.flush();
            if (videoEncoder) await videoEncoder.flush();
            if (audioEncoder) await audioEncoder.flush();

            muxer.finalize();

            const resultBlob = new Blob([muxer.target.buffer], { type: 'video/mp4' });

            const result = {
                fileId: id,
                originalName: file.name,
                canonicalName: file.name.replace(/\.[^/.]+$/, "") + ".mp4",
                canonicalBlob: resultBlob,
                mimeType: 'video/mp4',
                metadata: {
                    originalSize: file.size,
                    finalSize: resultBlob.size,
                    compressionRatio: Number((resultBlob.size / file.size).toFixed(3)),
                    preset: config.id
                }
            };

            self.postMessage({ type: 'DONE', data: result });

        } catch (err) {
            console.error("[MIE VideoWorker] Fallo Crítico:", err);
            self.postMessage({ type: 'ERROR', error: err.message });
        }
    }
};
