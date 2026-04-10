/**
 * video_ingest_worker.js — v4.69
 * Blindaje Safari: Extracción de Parameter Sets para decodificación exitosa en iOS.
 */
import * as Mp4BoxPkg from 'mp4box';
import * as Mp4Muxer from 'mp4-muxer';

const MP4Box = Mp4BoxPkg.default || Mp4BoxPkg;

self.onmessage = async (e) => {
    const { type, file, config, id } = e.data;

    if (type === 'INGEST') {
        try {
            console.log(`[MIE VideoWorker] Iniciando Ingesta: ${file.name}`);

            let muxer;
            let videoEncoder;
            let audioEncoder;
            let videoConfigured = false;

            const setupEncoders = (vTrack, aTrack) => {
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
                    video: { codec: 'avc', width, height },
                    fastStart: 'in-memory' 
                });

                videoEncoder = new VideoEncoder({
                    output: (chunk, metadata) => {
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
                        error: (e) => console.error("[MIE AudioEncoder] Error:", err)
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

                // --- HACK PARA SAFARI/IPHONE: Extracción de Description (SPS/PPS) ---
                const rawTrack = mp4boxfile.getTrackById(vTrack.id);
                let description = null;
                if (rawTrack && rawTrack.mdia && rawTrack.mdia.minf && rawTrack.mdia.minf.stbl && rawTrack.mdia.minf.stbl.stsd) {
                    const entry = rawTrack.mdia.minf.stbl.stsd.entries[0];
                    if (entry.avcC) {
                        const box = entry.avcC;
                        const stream = new MP4Box.DataStream(undefined, 0, MP4Box.DataStream.BIG_ENDIAN);
                        box.write(stream);
                        description = new Uint8Array(stream.buffer, 8); // Saltamos el header de la box
                    } else if (entry.hvcC) {
                        const box = entry.hvcC;
                        const stream = new MP4Box.DataStream(undefined, 0, MP4Box.DataStream.BIG_ENDIAN);
                        box.write(stream);
                        description = new Uint8Array(stream.buffer, 8);
                    }
                }

                videoDecoder.configure({
                    codec: vTrack.codec,
                    width: vTrack.video.width,
                    height: vTrack.video.height,
                    description: description // Esto es lo que Safari necesita para no fallar
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
                    const isVideo = trackId === mp4boxfile.getInfo().videoTracks[0].id;

                    if (isVideo && videoDecoder) {
                        videoDecoder.decode(new EncodedVideoChunk({
                            type: sample.is_sync ? 'key' : 'delta',
                            timestamp, duration, data: sample.data
                        }));
                    } else if (!isVideo && audioDecoder) {
                        audioDecoder.decode(new EncodedAudioChunk({
                            type: 'key',
                            timestamp, duration, data: sample.data
                        }));
                    }
                }
            };

            const CHUNK_SIZE = 10 * 1024 * 1024;
            let offset = 0;
            while (offset < file.size) {
                const end = Math.min(offset + CHUNK_SIZE, file.size);
                const arrayBuffer = await file.slice(offset, end).arrayBuffer();
                arrayBuffer.fileStart = offset;
                mp4boxfile.appendBuffer(arrayBuffer);
                offset = end;
                if (offset % (CHUNK_SIZE * 5) === 0) await new Promise(r => setTimeout(r, 0));
            }
            mp4boxfile.flush();

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
