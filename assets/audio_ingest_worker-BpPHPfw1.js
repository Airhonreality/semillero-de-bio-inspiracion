/**
 * =============================================================================
 * audio_ingest_worker.js
 * ORIGEN: Normalización de fuentes sonoras para el Cosmos de Indra.
 * RESOLUCIÓN: Transcodificación WebCodecs a AAC-LC (M4A) local.
 * AXIOMA: Normalización y compresión canónica sin servidores intermedios.
 * LEY DE ADUANA (ADR-008): Solo audio normalizado entra al sistema.
 * =============================================================================
 */

import * as Mp4Muxer from 'mp4-muxer';
// mp4box no es estrictamente necesario para audio puro si usamos WebAudio o decodificación directa,
// pero si es un .m4a o .mp4 con audio, lo necesitaremos para demuxear.
import * as Mp4BoxPkg from 'mp4box';
const MP4Box = Mp4BoxPkg.default || Mp4BoxPkg;

self.onmessage = async (e) => {
    const { type, file, config, id } = e.data;

    if (type === 'INGEST') {
        try {
            // 1. Preparar Muxer de Salida
            const muxer = new Mp4Muxer.Muxer({
                target: new Mp4Muxer.ArrayBufferTarget(),
                audio: {
                    codec: 'aac',
                    numberOfChannels: config.audio.numberOfChannels || 1, // Mono por defecto para eficiencia
                    sampleRate: config.audio.sample_rate || 44100
                },
                fastStart: 'in-memory'
            });

            // 2. Preparar Encoder
            const encoder = new AudioEncoder({
                output: (chunk, metadata) => muxer.addAudioChunk(chunk, metadata),
                error: (err) => console.error("[MIE AudioEncoder] Error:", err)
            });

            encoder.configure({
                codec: config.audio.codec || 'mp4a.40.2',
                numberOfChannels: config.audio.numberOfChannels || 1,
                sampleRate: config.audio.sample_rate || 44100,
                bitrate: config.audio.bitrate || 128000
            });

            // 3. Decodificación (Vía WebAudio o Demuxer si es MP4/M4A)
            // Para simplicidad en esta fase MIE, usamos el Demuxer MP4Box si es m4a/mp4
            // Si es mp3/wav, se requeriría un decodificador diferente o bridge al main thread.
            // Axioma de Realismo: La mayoría de audio moderno viene en contenedores mp4/m4a.
            
            const processAudioFile = () => new Promise(async (resolve, reject) => {
                const mp4boxfile = MP4Box.createFile();
                let totalSamples = 0;
                let decodedSamples = 0;
                let allSamplesDispatched = false;

                mp4boxfile.onReady = (info) => {
                    const audioTrack = info.audioTracks[0];
                    if (!audioTrack) return reject(new Error("No audio track found"));
                    
                    totalSamples = audioTrack.nb_samples;

                    const decoder = new AudioDecoder({
                        output: async (audioData) => {
                            encoder.encode(audioData);
                            audioData.close();
                            decodedSamples++;
                            // FIX B: Resolver solo cuando TODOS los samples están procesados
                            if (allSamplesDispatched && decodedSamples >= totalSamples) {
                                await encoder.flush();
                                resolve();
                            }
                        },
                        error: (err) => reject(err)
                    });

                    decoder.configure({
                        codec: audioTrack.codec,
                        numberOfChannels: audioTrack.audio.channel_count,
                        sampleRate: audioTrack.audio.sample_rate
                    });

                    mp4boxfile.setExtractionOptions(audioTrack.id);
                    mp4boxfile.start();
                };

                mp4boxfile.onSamples = (id, user, samples) => {
                    for (const sample of samples) {
                        decoder.decode(new EncodedAudioChunk({
                            type: 'key',
                            timestamp: (sample.cts / sample.timescale) * 1_000_000,
                            duration: (sample.duration / sample.timescale) * 1_000_000,
                            data: sample.data
                        }));
                    }
                };

                // FIX A: Streaming real por bloques de 10MB (no más arrayBuffer total)
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
                allSamplesDispatched = true;
            });

            await processAudioFile();
            muxer.finalize();

            const resultBlob = new Blob([muxer.target.buffer], { type: 'audio/mp4' });

            const result = {
                fileId: id,
                originalName: file.name,
                canonicalName: file.name.replace(/\.[^/.]+$/, "") + ".m4a",
                canonicalBlob: resultBlob,
                mimeType: 'audio/mp4',
                metadata: {
                    originalSize: file.size,
                    finalSize: resultBlob.size,
                    compressionRatio: Number((resultBlob.size / file.size).toFixed(3)),
                    preset: config.id
                }
            };

            self.postMessage({ type: 'DONE', data: result });

        } catch (err) {
            console.error("[MIE AudioWorker] Error:", err);
            self.postMessage({ type: 'ERROR', error: err.message });
        }
    }
};
