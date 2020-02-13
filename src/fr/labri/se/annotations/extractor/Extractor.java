package fr.labri.se.annotations.extractor;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.version.Version;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import fr.labri.se.aether.AetherDownloader;

public class Extractor {
	private final Driver driver;
	private final AetherDownloader downloader;
	private static final Logger logger = LogManager.getLogger(Extractor.class);

	public Extractor(Driver d, int qps) {
		driver = d;
		downloader = new AetherDownloader(qps);
	}

	public Stream<Path> fetchTopLibraries(int n) {
		try (Session session = driver.session(AccessMode.READ)) {
			StatementResult result = session.run(
				"MATCH (c)-[:DEPENDS_ON]->(l) " +
				"WITH DISTINCT l.groupID AS group, l.artifact AS artifact, size(collect(c)) AS clients " +
				"RETURN group, artifact " +
				"ORDER BY clients DESC " +
				"LIMIT {n} ",
				ImmutableMap.of("n", n)
			);

			Stream.Builder<Path> builder = Stream.<Path>builder();
			while (result.hasNext()) {
				Record row = result.next();
				Artifact a = new DefaultArtifact(String.format("%s:%s:[0,)",
					row.get("group").asString(),
					row.get("artifact").asString()
				));

				Version latest = downloader.retrieveLatestVersion(a);
				if (a != null && latest != null) {
					Artifact dl = downloader.downloadArtifact(a.setVersion(latest.toString()));
					if (dl != null && dl.getFile().exists())
						builder.add(dl.getFile().toPath());
					else
						logger.warn("Couldn't download " + dl);
				}
			}

			return builder.build();
		} catch (Exception e) {
			logger.error("E:", e);
			return Stream.empty();
		}
	}

	public Set<String> extractAnnotations(Path jar) {
		Set<String> ret = Sets.newHashSet();

		try (InputStream is = Files.newInputStream(jar); JarInputStream jarStream = new JarInputStream(is)) {
			JarEntry entry;
			while ((entry = jarStream.getNextJarEntry()) != null) {
				if (entry.getName().endsWith(".class")) {
					ClassReader cr = new ClassReader(jarStream);
					cr.accept(new ClassVisitor(Opcodes.ASM5) {
						@Override
						public AnnotationVisitor visitAnnotation(final String descriptor, final boolean visible) {
							ret.add(descriptor);
							return null;
						}

						@Override
						public MethodVisitor visitMethod(final int access, final String name, final String descriptor, final String signature, final String[] exceptions) {
							return new MethodVisitor(Opcodes.ASM5) {
								@Override
								public AnnotationVisitor visitAnnotation(final String descriptor, final boolean visible) {
									ret.add(descriptor);
									return null;
								}
							};
						}

						@Override
						public FieldVisitor visitField(final int access, final String name, final String descriptor, final String signature, final Object value) {
							return new FieldVisitor(Opcodes.ASM5) {
								@Override
								public AnnotationVisitor visitAnnotation(final String descriptor, final boolean visible) {
									ret.add(descriptor);
									return null;
								}
							};
						}
					}, ClassReader.SKIP_CODE & ClassReader.SKIP_DEBUG & ClassReader.SKIP_FRAMES);
				}
			}
		} catch (Exception e) {
			logger.error("E:", e);
		}

		return ret;
	}

	public static void main(String[] args) {
		String user = "neo4j";
		String pwd  = "neo4j";
		String host = "bolt://localhost:7687";
		int n = 1000;
		int qps = 15;

		try (Driver driver = GraphDatabase.driver(host, AuthTokens.basic(user, pwd))) {
			Extractor extractor = new Extractor(driver, qps);
			Stream<Path> topLibs = extractor.fetchTopLibraries(n);

			topLibs
				.map(f -> extractor.extractAnnotations(f))
				.reduce((s1, s2) -> { s1.addAll(s2); return s1; })
				.get()
				.forEach(System.out::println);
		} catch (Exception e) {
			logger.error("E:", e);
		}
	}
}
