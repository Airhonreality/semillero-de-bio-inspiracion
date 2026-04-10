/**
 * video_ingest_worker.js — v4.71
 * Diagnóstico Activo: Logs de seguimiento para rastrear el fallo de colorSpace.
 */
import * as Mp4BoxPkg from 'mp4box';
import * as Mp4Muxer from 'mp4-muxer';

const MP4Box = Mp4BoxPkg.default || Mp4BoxPkg;

self.onmessage = async (e) => {
    const { type, file, config, id } = e.data;

    if (type === 'INGEST') {
        try {
            console.log(`[MIE Worker] 1. Iniciando: ${file.name} (${file.size} bytes)`);

            let muxer;
            let videoEncoder;
            let audioEncoder;
            let chunksEncoded = 0;

            const setupEncoders = (vTrack, aTrack) => {
                console.log(`[MIE Worker] 3. Configurando Encoders. Codec original: ${vTrack.codec}`);
                let width = vTrack.video.width;
                let height = vTrack.video.height;
                const maxRes = config.video.max_resolution || 1080;

                if (height > maxRes) {
                    width = Math.round((width * maxRes) / height);
                    height = maxRes;
                }
                
                width = width % 2 === 0 ? width : width + 1;
                height = height % 2 === 0 ? height : height + 1;

                muxer = new Mp4Muxer.Muxer({
                    target: new Mp4Muxer.ArrayBufferTarget(),
                    video: { 
                        codec: 'avc', 
                        width, 
                        height,
                        colorSpace: { primary: 'bt709', transfer: 'bt709', matrix: 'bt709', fullRange: true }
                    },
                    fastStart: 'in-memory' 
                });

                videoEncoder = new VideoEncoder({
                    output: (chunk, metadata) => {
                        chunksEncoded++;
                        if (metadata && metadata.decoderConfig) {
                            muxer.addVideoChunk(chunk, metadata);
                        } else {
                            muxer.addVideoChunk(chunk);
                        }
                    },
                    error: (e) => console.error("[MIE VideoEncoder] Error:", e)
                });

                videoEncoder.configure({
                    codec: config.video.codec || 'avc1.4d002a',
                    width, height,
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

                return { width, height };
            };

            const mp4boxfile = MP4Box.createFile();
            let videoDecoder;
            let audioDecoder;
            let canvas;
            let ctx;

            mp4boxfile.onReady = (info) => {
                console.log("[MIE Worker] 2. MP4Box Listo. Tracks:", info.videoTracks.length);
                const vTrack = info.videoTracks[0];
                const aTrack = info.audioTracks[0];
                if (!vTrack) throw new Error("No video track found");

                const { width, height } = setupEncoders(vTrack, aTrack);
                canvas = new OffscreenCanvas(width, height);
                ctx = canvas.getContext('2d');

                videoDecoder = new VideoDecoder({
                    output: (frame) => {
                        ctx.drawImage(frame, 0, 0, width, height);
                        const newFrame = new VideoFrame(canvas, { timestamp: frame.timestamp });
                        videoEncoder.encode(newFrame);
                        newFrame.close();
                        frame.close();
                    },
                    error: (e) => console.error("[MIE VideoDecoder] Error:", e)
                });

                videoDecoder.configure({
                    codec: vTrack.codec,
                    width: vTrack.video.width,
                    height: vTrack.video.height
                });

                if (aTrack && audioEncoder) {
                    audioDecoder = new AudioDecoder({
                        output: (data) => { audioEncoder.encode(data); data.close(); },
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
                    if (videoDecoder) {
                        videoDecoder.decode(new EncodedVideoChunk({
                            type: sample.is_sync ? 'key' : 'delta',
                            timestamp, duration, data: sample.data
                        }));
                    }
                }
            };

            const CHUNK_SIZE = 5 * 1024 * 1024;
            let offset = 0;
            while (offset < file.size) {
                const end = Math.min(offset + CHUNK_SIZE, file.size);
                const arrayBuffer = await file.slice(offset, end).arrayBuffer();
                arrayBuffer.fileStart = offset;
                mp4boxfile.appendBuffer(arrayBuffer);
                offset = end;
            }
            mp4boxfile.flush();

            console.log("[MIE Worker] 4. Esperando vaciado de encoders...");
            if (videoDecoder) await videoDecoder.flush();
            if (audioDecoder) await audioDecoder.flush();
            if (videoEncoder) await videoEncoder.flush();
            if (audioEncoder) await audioEncoder.flush();

            console.log(`[MIE Worker] 5. Finalizando Muxer. Chunks codificados: ${chunksEncoded}`);
            if (chunksEncoded === 0) {
                throw new Error("El codificador no produjo ningún fragmento de video. ¿Archivo corrupto o codec no soportado?");
            }

            muxer.finalize();
            const resultBlob = new Blob([muxer.target.buffer], { type: 'video/mp4' });
            
            self.postMessage({ type: 'DONE', data: {
                fileId: id,
                originalName: file.name,
                canonicalBlob: resultBlob,
                mimeType: 'video/mp4'
            }});

        } catch (err) {
            console.error("[MIE VideoWorker] Fallo Crítico:", err);
            self.postMessage({ type: 'ERROR', error: err.message });
        }
    }
};
